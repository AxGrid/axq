/*
 * Created by Zed 06.12.2023, 19:34
 */

package service

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/go-errors/errors"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/speps/go-hashids"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
)

// blobUploader кладёт упакованный архив в хранилище. Вынесен в интерфейс, чтобы
// тесты гоняли конвейер целиком, не выходя в сеть.
type blobUploader interface {
	Upload(filename string, metadata map[string]string, data []byte) error
}

type b2Uploader struct {
	bucket *backblaze.Bucket
}

func (u *b2Uploader) Upload(filename string, metadata map[string]string, data []byte) error {
	_, err := u.bucket.UploadFile(filename, metadata, bytes.NewBuffer(data))
	return err
}

type ArchiverService struct {
	opts        domain.ArchiverOptions
	uploader    blobUploader
	db          *gorm.DB
	logger      zerolog.Logger
	tableName   string
	counters    *CounterService
	dbAes       *utils.AES
	b2Aes       *utils.AES
	b2Bucket    *backblaze.Bucket
	hashId      *hashids.HashID
	reader      *ReaderService
	ctx         context.Context
	cancelFn    context.CancelFunc
	outChan     chan *protobuf.Blob
	currentBlob *protobuf.Blob
	messageList *protobuf.BlobMessageList
	rawSize     int64
	packRatio   float64
	blobIdsChan chan domain.BlobIDs
	b2Fid       uint64
	// currentDbFid — fid блоба БД, из которого пришло последнее уложенное
	// сообщение; принадлежит горутине loader
	currentDbFid uint64
	// archivedDbFid — последний fid блоба БД, целиком уехавший в B2. Пишется
	// в sorter после коммита, читается снаружи, поэтому atomic.
	archivedDbFid uint64

	cleanMu         sync.Mutex
	cleanStats      domain.CleanStats
	cleanDeleted    int64
	cleanLastDelFid uint64
}

const (
	defaultCleanInterval = time.Minute
	defaultCleanBatch    = 1000
)

// archiverCounterName — имя строки счётчика архивера. Её fid означает ровно то
// же, что у любого другого потребителя очереди — позицию в таблице, — поэтому
// из расчёта самого отставшего её исключать не нужно.
func archiverCounterName(queue string) string {
	return fmt.Sprintf("b2_archiver_%s", queue)
}

// ArchivedDbFID возвращает блоб таблицы, в котором лежит последнее уехавшее в
// B2 сообщение. Сам этот блоб мог уехать не целиком — его хвост попадёт в
// следующий архив, — поэтому удалять можно только то, что строго ниже.
func (a *ArchiverService) ArchivedDbFID() uint64 {
	return atomic.LoadUint64(&a.archivedDbFid)
}

func NewArchiverService(opts domain.ArchiverOptions) (*ArchiverService, error) {
	return newArchiverService(opts, nil)
}

// newArchiverService собирает сервис с заданной заливкой. Пустой uploader
// означает боевой путь: авторизоваться в B2 и работать с бакетом.
func newArchiverService(opts domain.ArchiverOptions, uploader blobUploader) (*ArchiverService, error) {
	ctx, cancelFn := context.WithCancel(opts.CTX)
	r := &ArchiverService{
		opts:        opts,
		logger:      opts.Logger.With().Str("name", opts.Name).Logger(),
		db:          opts.DB.DB,
		ctx:         ctx,
		cancelFn:    cancelFn,
		packRatio:   1,
		outChan:     make(chan *protobuf.Blob, outChanSize(opts.OuterCount)),
		blobIdsChan: make(chan domain.BlobIDs, 1000),
	}
	//if opts.B2.Endpoint == "" {
	//	return nil, errors.New("b2 endpoint empty")
	//}
	if opts.B2.Salt != "" {
		hashIdData := hashids.NewData()
		hashIdData.Salt = opts.B2.Salt
		hashIdData.MinLength = 10
		var err error
		r.hashId, err = hashids.NewWithData(hashIdData)
		if err != nil {
			return nil, err
		}
	}
	bucketName := fmt.Sprintf("axq-%s-%s", opts.Prefix, utils.GetMD5Hash([]byte(opts.Name), opts.B2.Salt))
	if len(bucketName) > 63 {
		bucketName = bucketName[:63]
	}
	var err error
	if uploader == nil {
		r.b2Bucket, err = r.createBucket(bucketName)
		if err != nil {
			return nil, err
		}
		uploader = &b2Uploader{bucket: r.b2Bucket}
	}
	r.uploader = uploader
	r.tableName = fmt.Sprintf("axq_%s", opts.Name)
	if !r.db.Migrator().HasTable(r.tableName) {
		opts.Logger.Debug().Str("table", r.tableName).Msg("create table")
		if err := r.db.Table(r.tableName).AutoMigrate(domain.Blob{}); err != nil {
			return nil, errors.New(fmt.Sprintf("fail migrate table:(%s): %s", r.tableName, err))
		}
	}
	if opts.DB.Compression.Encryption == domain.BLOB_ENCRYPTION_AES {
		if len(opts.DB.Compression.EncryptionKey) != 32 {
			return nil, errors.New("invalid encryption key size")
		}
		aes := utils.NewAES(opts.DB.Compression.EncryptionKey)
		_, err := aes.Encrypt([]byte("test"))
		if err != nil {
			return nil, err
		}
		r.dbAes = aes
	}

	if opts.B2.Compression.Encryption == domain.BLOB_ENCRYPTION_AES {
		if len(opts.B2.Compression.EncryptionKey) != 32 {
			return nil, errors.New("invalid encryption key size")
		}
		aes := utils.NewAES(opts.B2.Compression.EncryptionKey)
		_, err := aes.Encrypt([]byte("test"))
		if err != nil {
			return nil, err
		}
		r.b2Aes = aes
	}
	archiverName := archiverCounterName(opts.Name)
	r.counters, err = NewCounterService(opts.Name, archiverName, opts.CTX, opts.Logger, r.db, false, false, false) // B2 Counters
	if err != nil {
		return nil, err
	}
	lastId, err := r.counters.Get()
	if err != nil {
		return nil, err
	}
	r.b2Fid = lastId.B2Fid
	atomic.StoreUint64(&r.archivedDbFid, lastId.FID)
	r.logger.Info().Uint64("last-id", lastId.Id).Uint64("start-fid", lastId.FID).Uint64("b2-fid", lastId.B2Fid).Msg("archiver start position")
	readerName := fmt.Sprintf("%s_reader", archiverName)
	r.reader, err = NewReaderService(domain.ReaderOptions{
		BaseOptions:  opts.BaseOptions,
		ReaderName:   readerName,
		DB:           opts.DB,
		BufferSize:   100_000,
		BatchSize:    1000,
		LoaderCount:  opts.Reader.LoaderCount,
		WaiterCount:  opts.Reader.WaiterCount,
		StartFromEnd: opts.Reader.StartFromEnd,
		LastId: &domain.LastIdOptions{
			FID:    lastId.FID,
			LastId: lastId.Id,
		},
	})
	if err != nil {
		return nil, err
	}
	go r.loader(0)
	go r.sorter()
	for i := 0; i < opts.OuterCount; i++ {
		go r.outer(i)
	}
	if opts.CleanGapFID > 0 {
		go r.cleaner()
	}

	return r, nil
}

// CleanStats отдаёт снимок последнего прохода чистки. Сама чистка молча стоит,
// если какой-то потребитель встал, поэтому отставание надо мониторить снаружи.
func (a *ArchiverService) CleanStats() domain.CleanStats {
	a.cleanMu.Lock()
	defer a.cleanMu.Unlock()
	return a.cleanStats
}

func (a *ArchiverService) cleaner() {
	interval := a.opts.CleanInterval
	if interval <= 0 {
		interval = defaultCleanInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if err := a.clean(); err != nil {
				a.logger.Error().Err(err).Msg("fail to clean archived blobs")
			}
		}
	}
}

// clean удаляет из таблицы то, что уже прошли все потребители, оставляя позади
// самого отставшего зазор в CleanGapFID блобов.
func (a *ArchiverService) clean() error {
	head, err := a.headFID()
	if err != nil {
		return err
	}
	slowName, slowFid, hasReaders, err := a.slowestConsumer()
	if err != nil {
		return err
	}

	stats := domain.CleanStats{
		SlowestReader:    slowName,
		SlowestReaderFID: slowFid,
		HeadFID:          head,
		ArchivedFID:      a.ArchivedDbFID(),
		LastRun:          time.Now(),
		NoReaders:        !hasReaders,
	}
	defer func() {
		a.cleanMu.Lock()
		stats.DeletedRows = a.cleanDeleted
		stats.LastDeletedFID = a.cleanLastDelFid
		a.cleanStats = stats
		a.cleanMu.Unlock()
	}()

	// ни одного потребителя — удалять не за кем и опасно
	if !hasReaders || slowFid <= a.opts.CleanGapFID {
		return nil
	}
	deleteTo := slowFid - a.opts.CleanGapFID

	a.cleanMu.Lock()
	already := a.cleanLastDelFid
	a.cleanMu.Unlock()
	if deleteTo <= already {
		return nil
	}

	deleted, err := a.deleteUpTo(deleteTo)
	a.cleanMu.Lock()
	a.cleanDeleted += deleted
	if deleted > 0 {
		a.cleanLastDelFid = deleteTo
	}
	a.cleanMu.Unlock()
	if err != nil {
		return err
	}
	if deleted > 0 {
		a.logger.Info().
			Int64("rows", deleted).
			Uint64("up-to-fid", deleteTo).
			Str("slowest", slowName).
			Uint64("slowest-fid", slowFid).
			Uint64("head-fid", head).
			Msg("cleaned archived blobs")
	}
	return nil
}

// slowestConsumer — потребитель очереди с наименьшей позицией. Строка архивера
// участвует наравне с остальными: её fid означает то же самое, и именно это
// не даёт чистке обогнать заливку в B2.
func (a *ArchiverService) slowestConsumer() (string, uint64, bool, error) {
	var row struct {
		ReaderName string
		Fid        uint64
	}
	res := a.db.Model(&domain.BlobCounter{}).
		Select("reader_name", "fid").
		Where("name = ?", a.opts.Name).
		Order("fid asc").
		Limit(1).
		Scan(&row)
	if res.Error != nil {
		return "", 0, false, res.Error
	}
	return row.ReaderName, row.Fid, res.RowsAffected > 0, nil
}

func (a *ArchiverService) headFID() (uint64, error) {
	var blob domain.Blob
	err := a.db.Table(a.tableName).Select("fid").Order("fid desc").First(&blob).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return blob.FID, nil
}

// deleteUpTo удаляет блобы батчами: одним DELETE на сотни тысяч строк мы бы
// держали длинную транзакцию и лочили таблицу под живой записью.
func (a *ArchiverService) deleteUpTo(fid uint64) (int64, error) {
	batch := a.opts.CleanBatch
	if batch <= 0 {
		batch = defaultCleanBatch
	}
	var total int64
	for {
		select {
		case <-a.ctx.Done():
			return total, nil
		default:
		}
		res := a.db.Table(a.tableName).Where("fid <= ?", fid).Limit(batch).Delete(&domain.Blob{})
		if res.Error != nil {
			return total, res.Error
		}
		total += res.RowsAffected
		if res.RowsAffected < int64(batch) {
			return total, nil
		}
	}
}

func (a *ArchiverService) loader(index int) {
	wlog := a.logger.With().Int("archiver loader", index).Logger()
	wlog.Debug().Msg("start archiver loader")
	for {
		select {
		case <-a.ctx.Done():
			return
		case msg := <-a.reader.C():
			if msg.Id() <= a.counters.Last().Id {
				// уже заархивировано: подтвердить обязательно, иначе воркер
				// ридера навсегда останется висеть на своём ack
				msg.Done()
				continue
			}
			if a.currentBlob == nil {
				fid := atomic.AddUint64(&a.b2Fid, 1)
				wlog.Info().Uint64("fid", fid).Msg("new blob")
				a.currentBlob = &protobuf.Blob{
					FromId:      msg.Id(),
					Fid:         fid,
					Compression: protobuf.BlobCompression(a.opts.B2.Compression.Compression),
					Encryption:  protobuf.BlobEncryption(a.opts.B2.Compression.Encryption),
				}
				a.messageList = &protobuf.BlobMessageList{
					Fid: fid,
				}
				a.rawSize = 0
			}
			a.currentBlob.ToId = msg.Id()
			// запоминаем, из какого блоба БД приехало сообщение: по нему
			// потом видно, докуда таблица заархивирована целиком
			a.currentDbFid = msg.Fid()
			blobMessage := &protobuf.BlobMessage{
				Id:      msg.Id(),
				Message: msg.Message(),
			}
			a.messageList.Messages = append(a.messageList.Messages, blobMessage)
			a.rawSize += int64(proto.Size(blobMessage))
			msg.Done()

			if !a.readyToPack() {
				continue
			}
			packed, err := a.packBlob()
			if err != nil {
				wlog.Error().Err(err).Msg("fail pack blob")
				continue
			}
			if int64(len(packed)) <= int64(a.opts.MaxSize) && len(a.messageList.Messages) < a.opts.MaxCount {
				continue
			}
			a.currentBlob.Messages = packed
			a.currentBlob.Count = uint64(len(a.messageList.Messages))
			a.currentBlob.DbFid = a.currentDbFid
			a.outChan <- a.currentBlob
			wlog.Info().Int("size", len(packed)).Msgf("send blob %d", a.currentBlob.Fid)
			a.currentBlob = nil
			a.messageList = nil
			a.rawSize = 0
		}
	}
}

func (a *ArchiverService) outer(index int) {
	wlog := a.logger.With().Int("archiver-out-worker", index).Logger()
	wlog.Debug().Msg("start outer")
	for {
		select {
		case <-a.ctx.Done():
			return
		case m := <-a.outChan:
			bts, err := proto.Marshal(m)
			if err != nil {
				wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to marshall proto")
				continue
			}
			metadata := make(map[string]string)
			if a.hashId != nil {
				h, err := a.hashId.EncodeInt64([]int64{int64(m.FromId), int64(m.ToId), int64(m.Count), int64(m.Compression), int64(m.Encryption)})
				if err != nil {
					wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to get metadata")
					continue
				}
				metadata["hash"] = h
			} else {
				metadata["hash"] = fmt.Sprintf("%d-%d-%d-%d", m.FromId, m.ToId, m.Count, int32(m.Compression))
			}
			filename, err := utils.GetBlobFileName(a.hashId, a.opts.Name, m.Fid)
			if err != nil {
				wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to get filename")
				continue
			}
			//blobMD5 := utils.GetMD5Hash(bts, "")
			for {
				err = a.uploader.Upload(filename, metadata, bts)
				if err != nil {
					wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail upload blob")
					select {
					case <-a.ctx.Done():
						return
					case <-time.After(100 * time.Millisecond):
					}
					continue
				}

				//for i := 0; i < 5; i++ {
				//	b2File, err := utils.DownloadFileByName(a.b2Bucket.Name, a.opts.B2.Endpoint, filename)
				//	if err != nil {
				//		wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to check uploaded file")
				//		time.Sleep(100 * time.Millisecond)
				//		continue
				//	}
				//
				//	if utils.GetMD5Hash(b2File, "") != blobMD5 {
				//		wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to check md5")
				//		time.Sleep(100 * time.Millisecond)
				//		continue
				//	}
				//}
				wlog.Info().Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Str("filename", filename).Msg("successfully uploaded")
				select {
				case <-a.ctx.Done():
					return
				case a.blobIdsChan <- domain.BlobIDs{
					FID:    m.Fid,
					FromId: m.FromId,
					ToId:   m.ToId,
					DbFid:  m.DbFid,
				}:
				}
				break
			}

		}
	}
}

// sorter коммитит залитые блобы строго по возрастанию id. Воркеры outer
// заливают параллельно и приходят сюда в произвольном порядке, поэтому блоб с
// разрывом ждёт в waitMap, а не возвращается обратно в канал.
//
// {FID: 200, FromId: 1000, ToId: 1010}
// {FID: 201, FromId: 1011, ToId: 1020}
func (a *ArchiverService) sorter() {
	waitMap := map[uint64]domain.BlobIDs{}
	nextId := a.counters.Last().Id + 1
	for {
		select {
		case <-a.ctx.Done():
			return
		case ids := <-a.blobIdsChan:
			waitMap[ids.FromId] = ids
			for {
				next, ok := waitMap[nextId]
				if !ok {
					break
				}
				delete(waitMap, next.FromId)
				// диапазон блоба непрерывен, а сами блобы коммитятся по
				// порядку, поэтому счётчик двигается одним шагом на весь блоб
				// В fid счётчика идёт позиция в таблице — тот же смысл, что у
				// всех остальных потребителей очереди. Номер файла в архиве
				// живёт отдельным полем: это разные пространства нумерации.
				a.counters.Commit(domain.MessageIDs{
					FID:   next.DbFid,
					B2Fid: next.FID,
					Id:    next.ToId,
				})
				nextId = next.ToId + 1
				if next.DbFid > 0 {
					atomic.StoreUint64(&a.archivedDbFid, next.DbFid)
				}
				a.logger.Info().Any("ids", next).Int("waiting", len(waitMap)).Msg("processed batch. set counters")
			}
		}
	}
}

func (a *ArchiverService) createBucket(bucketName string) (*backblaze.Bucket, error) {
	b2, err := backblaze.NewB2(a.opts.B2.Credentials)
	if err != nil {
		return nil, errors.New("fail to create B2: " + err.Error())
	}
	a.logger.Info().Interface("creds", a.opts.B2.Credentials).Msg("authorize B2")
	if err = b2.AuthorizeAccount(); err != nil {
		return nil, errors.New("fail to authorize B2: " + err.Error())
	}

	bucket, err := b2.Bucket(bucketName)
	if err != nil || bucket == nil {
		bucket, err = b2.CreateBucket(bucketName, backblaze.AllPublic)
		a.logger.Info().Msg("create bucket")
		if err != nil {
			return nil, err
		}
	} else {
		a.logger.Info().Bool("has-bucket", bucket != nil).Msg("connected to bucket")
	}

	return bucket, nil
}

// readyToPack сообщает, есть ли смысл упаковывать текущий блоб. Размер после
// сжатия и шифрования оценивается по packRatio, измеренному на предыдущей
// упаковке, поэтому блоб пакуется считанные разы, а не раз в ChunkSize
// сообщений.
func (a *ArchiverService) readyToPack() bool {
	if len(a.messageList.Messages) >= a.opts.MaxCount {
		return true
	}
	return float64(a.rawSize)*a.packRatio > float64(a.opts.MaxSize)
}

// packBlob маршалит, сжимает и шифрует накопленные сообщения и уточняет
// packRatio по фактическому результату.
func (a *ArchiverService) packBlob() ([]byte, error) {
	data, err := proto.Marshal(a.messageList)
	if err != nil {
		return nil, err
	}
	switch a.opts.B2.Compression.Compression {
	case domain.BLOB_COMPRESSION_GZIP:
		if data, err = utils.GZipData(data); err != nil {
			return nil, err
		}
	}
	switch a.opts.B2.Compression.Encryption {
	case domain.BLOB_ENCRYPTION_AES:
		if data, err = a.b2Aes.Encrypt(data); err != nil {
			return nil, err
		}
	}
	if a.rawSize > 0 {
		a.packRatio = float64(len(data)) / float64(a.rawSize)
	}
	return data, nil
}

// outChanSize ограничивает число готовых блобов, ожидающих заливки: каждый из
// них держит в памяти полный упакованный payload размером до MaxSize.
func outChanSize(outerCount int) int {
	if outerCount < 1 {
		return 2
	}
	return outerCount * 2
}

func (a *ArchiverService) Close() {
	a.cancelFn()
}
