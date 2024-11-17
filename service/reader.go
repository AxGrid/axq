/*
 * Created by Zed 06.12.2023, 09:38
 */

package service

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/go-errors/errors"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/speps/go-hashids"
	"golang.org/x/exp/constraints"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
	"sync"
	"sync/atomic"
	"time"
)

type T constraints.Integer

type ReaderService struct {
	opts           domain.ReaderOptions
	logger         zerolog.Logger
	counters       *CounterService
	tableName      string
	lastId         *utils.MinimalId[uint64]
	sortIdChan     chan uint64
	dbFid          uint64
	b2Fid          uint64
	fidLock        *FidLock
	b2FidLock      *FidLock
	blobListChan   chan *protobuf.BlobMessageList
	bufferChan     chan *protobuf.BlobMessage
	outChan        chan domain.Message
	db             *gorm.DB
	batchSize      uint64
	nextBatchSize  uint64
	b2Bucket       *backblaze.Bucket
	b2Auth         string
	hashId         *hashids.HashID
	dbAes          *utils.AES
	cancelFunc     context.CancelFunc
	ctx            context.Context
	stopped        bool
	deltaTime      int64
	deltaTimeCount int64
	performance    uint64
}

func (r *ReaderService) GetOpts() domain.ServiceOpts {
	return &r.opts
}

func (r *ReaderService) GetName() string {
	return r.opts.ReaderName
}

func (r *ReaderService) GetWriterName() string {
	return r.opts.Name
}

func (r *ReaderService) Counter() (uint64, error) {
	return r.counters.Get()
}

func (r *ReaderService) Performance() uint64 {
	return r.performance
}

func (r *ReaderService) Pop() domain.Message {
	return <-r.outChan
}

func (r *ReaderService) C() <-chan domain.Message {
	return r.outChan
}

func (r *ReaderService) LastFID() (uint64, error) {
	return r.dbFid, nil
}

func (r *ReaderService) LastID() (uint64, error) {
	return r.lastId.Current(), nil
}

func (r *ReaderService) GetWaitersCount() int {
	return r.opts.WaiterCount
}

func NewReaderService(opts domain.ReaderOptions) (*ReaderService, error) {
	if opts.ReaderName == "" {
		opts.ReaderName = fmt.Sprintf("%s_%s", opts.Name, utils.UUIDString())
	}
	ctx, cancelFn := context.WithCancel(opts.CTX)
	r := &ReaderService{
		opts:          opts,
		logger:        opts.Logger.With().Str("name", opts.Name).Str("reader", opts.ReaderName).Logger(),
		blobListChan:  make(chan *protobuf.BlobMessageList, opts.LoaderCount*opts.LoaderCount),
		bufferChan:    make(chan *protobuf.BlobMessage, opts.BufferSize),
		outChan:       make(chan domain.Message, opts.WaiterCount),
		db:            opts.DB.DB,
		batchSize:     opts.BatchSize,
		nextBatchSize: opts.BatchSize,
		ctx:           ctx,
		cancelFunc:    cancelFn,
		b2Bucket:      opts.B2Bucket,
	}
	var err error
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
	r.tableName = fmt.Sprintf("axq_%s", opts.Name)

	if !r.db.Migrator().HasTable(r.tableName) {
		opts.Logger.Debug().Str("table", r.tableName).Msg("create table")
		if err := r.db.Table(r.tableName).AutoMigrate(domain.Blob{}); err != nil {
			return nil, errors.New(fmt.Sprintf("fail migrate table:(%s): %s", r.tableName, err))
		}
	}

	if opts.B2Bucket == nil && r.opts.B2.Credentials.ApplicationKey != "" {
		r.b2Bucket, err = r.createBucket("axqueue-" + utils.GetMD5Hash([]byte(opts.Name), opts.B2.Salt))
		if err != nil {
			return nil, err
		}
	}

	if opts.B2.Salt != "" {
		hashIdData := hashids.NewData()
		hashIdData.Salt = opts.B2.Salt
		hashIdData.MinLength = 10
		r.hashId, err = hashids.NewWithData(hashIdData)
		if err != nil {
			return nil, err
		}
	}

	r.counters, err = NewCounterService(opts.Name, opts.ReaderName, opts.CTX, opts.Logger, r.db)
	if err != nil {
		return nil, err
	}
	if opts.LastId == nil {
		var lastId uint64
		lastId, err = r.counters.Get()
		if err != nil {
			return nil, err
		}
		r.lastId = utils.NewMinimalId(lastId)
		if r.lastId.Current() > 0 {
			var blob domain.Blob
			err = r.db.Table(r.tableName).Where("? >= from_id AND ? <= to_id", r.lastId.Current(), r.lastId.Current()).First(&blob).Error
			if err != nil {
				return nil, err
			}
			r.dbFid = blob.FID
		}
	} else {
		r.lastId = utils.NewMinimalId(opts.LastId.LastId)
		if r.lastId.Current() > 0 {
			var blob domain.Blob
			err = r.db.Table(r.tableName).Where("? >= from_id AND ? <= to_id", r.lastId.Current(), r.lastId.Current()).First(&blob).Error
			if err != nil {
				return nil, err
			}
			r.dbFid = blob.FID
		}
	}
	r.logger.Debug().Uint64("last-id", r.lastId.Current()).Uint64("fid", r.dbFid).Msg("init reader")
	if r.dbFid > 0 {
		r.dbFid--
	}
	r.fidLock = NewFidLock(r.dbFid)
	r.b2FidLock = NewFidLock(r.b2Fid)
	go r.createLoaders(ctx)
	go r.countPerformance()
	for i := 0; i < opts.WaiterCount; i++ {
		go r.outer(i)
	}
	return r, nil
}

func (r *ReaderService) createLoaders(ctx context.Context) {
	r.logger.Info().Msg("creating loaders")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if r.opts.B2.Credentials.ApplicationKey != "" {
				r.startB2Loaders()
			}
			r.startDBLoaders()
		}
	}

}

func (r *ReaderService) startB2Loaders() {
	b2Ctx, b2Cancel := context.WithCancel(context.Background())
	r.logger.Info().Msg("application key found. starting b2 loaders")
	wg := sync.WaitGroup{}
	for i := 0; i < r.opts.LoaderCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.loaderB2(i)
		}(i)
	}
	go r.b2Sorter(b2Ctx)
	r.logger.Info().Msg("waiting b2 loaders work done")
	wg.Wait()
	b2Cancel()
	r.logger.Info().Msg("b2 loaders work done")
}

func (r *ReaderService) startDBLoaders() {
	dbCtx, dbCancel := context.WithCancel(context.Background())
	r.logger.Info().Msg("starting db loaders")
	wg := sync.WaitGroup{}
	for i := 0; i < r.opts.LoaderCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.loaderDB(i)
		}(i)
	}
	go r.sorter(dbCtx)
	r.logger.Info().Msg("db loaders reading database")
	wg.Wait()
	dbCancel()
	r.logger.Warn().Msg("db loaders stopped")
}

func (r *ReaderService) loaderDB(index int) {
	wlog := r.logger.With().Int("db-loader-worker", index).Logger()
	wlog.Debug().Msg("start db loader")
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			if err := r.loadDB(index); err != nil {
				wlog.Error().Err(err).Msg("error while loading db. stopping")
				return
			}
		}
	}
}

func (r *ReaderService) loaderB2(index int) {
	wlog := r.logger.With().Int("b2-loader-worker", index).Logger()
	wlog.Debug().Msg("start b2 loader")
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			if err := r.loadB2(index); err != nil {
				wlog.Error().Err(err).Msg("error while loading b2. stopping")
				return
			}
		}
	}
}

func (r *ReaderService) loadDB(index int) error {
	wlog := r.logger.With().Int("db-loader-worker", index).Logger()
	fid := atomic.AddUint64(&r.dbFid, r.batchSize)
	var batch []domain.Blob
	for {
		err := r.getData(fid, &batch)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				wlog.Warn().Uint64("fid", fid).Msg("not found")
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				wlog.Warn().Err(err).Msg("load timeout")
				continue
			}
			wlog.Error().Err(err).Msg("load error")
			time.Sleep(250 * time.Millisecond)
			continue
		}
		wlog.Info().Int("batch-len", len(batch)).Msg("read batch")
		if len(batch) != int(r.batchSize) {
			if len(batch) == 0 {
				var blob domain.Blob
				if err = r.db.Table(r.tableName).Last(&blob).Error; err != nil {
					continue
				}
				wlog.Warn().Uint64("last-id", r.lastId.Current()).Uint64("last blob to-id", blob.ToId).Msg("empty batch")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			r.nextBatchSize -= uint64(len(batch))
		}
		for _, blob := range batch {
			wlog.Debug().Uint64("fid", fid).Uint64("from-id", blob.FromId).Uint64("to-id", blob.ToId).Msg("db blob")
			data := blob.Message
			switch blob.Encryption {
			case domain.BLOB_ENCRYPTION_AES:
				data, err = r.dbAes.Decrypt(blob.Message)
				if err != nil {
					wlog.Error().Err(err).Uint64("fid", blob.FID).Msg("decrypt blob error")
					continue
				}
			}
			switch blob.Compression {
			case domain.BLOB_COMPRESSION_GZIP:
				data, err = utils.GUnzipData(data)
				if err != nil {
					wlog.Error().Err(err).Uint64("fid", blob.FID).Msg("unzip blob error")
					continue
				}
			}
			var list protobuf.BlobMessageList
			err = proto.Unmarshal(data, &list)
			if err != nil {
				wlog.Error().Str("enсryption", blob.Encryption.String()).Str("comp", blob.Compression.String()).Err(err).Uint64("fid", blob.FID).Msg("unmarshal blob error")
				continue
			}
			list.Fid = blob.FID
			wlog.Debug().Uint64("fid", list.Fid).Msg("trying send to blob list chan")
			r.blobListChan <- &list
			wlog.Debug().Int64("delta-time", r.deltaTime).Int64("delta-count", r.deltaTimeCount).Int64("ratio", r.deltaTime/r.deltaTimeCount).Uint64("fid", list.Fid).Int("msg count", len(list.Messages)).Msg("success sending to blob list chan")
		}
		if len(batch) != int(r.batchSize) {
			if r.nextBatchSize == 0 {
				r.nextBatchSize = r.batchSize
				return nil
			}
			continue
		}
		return nil
	}
}

func (r *ReaderService) getData(fid uint64, res *[]domain.Blob) error {
	t := time.Now()
	localCtx, cancelFn := context.WithTimeout(r.ctx, time.Second*5)
	defer func() {
		cancelFn()
		atomic.AddInt64(&r.deltaTime, time.Since(t).Milliseconds())
		atomic.AddInt64(&r.deltaTimeCount, 1)
	}()
	return r.db.WithContext(localCtx).Table(r.tableName).Where("fid > ?", fid-r.nextBatchSize).Order("fid").Limit(int(r.batchSize)).Find(res).Error
}

func (r *ReaderService) loadB2(index int) error {
	wlog := r.logger.With().Int("b2-loader-worker", index).Logger()
	fid := atomic.AddUint64(&r.b2Fid, 1)
	for {
		filename, err := utils.GetBlobFileName(r.hashId, r.opts.Name, fid)
		if err != nil {
			wlog.Error().Err(err).Uint64("fid", fid).Msg("fail to get filename")
			continue
		}
		wlog.Info().Uint64("fid", fid).Str("filename", filename).Msg("try to download file")
		content, err := utils.DownloadFileByName(r.b2Bucket.Name, r.opts.B2.Endpoint, filename)
		if err != nil {
			if errors.Is(domain.ErrB2FileNotFound, err) {
				wlog.Warn().Uint64("fid", fid).Msg("fid not found")
				return err
			} else {
				wlog.Error().Err(err).Uint64("fid", fid).Msg("download file error")
			}
			continue
		}
		var blob protobuf.Blob
		err = proto.Unmarshal(content, &blob)
		if err != nil {
			wlog.Error().Str("enc", r.opts.B2.Compression.Encryption.String()).Err(err).Uint64("fid", fid).Msg("unmarshal blob error")
			continue
		}
		wlog.Info().Uint64("fid", fid).Str("filename", filename).Msg("successfully downloaded file")
		switch blob.Encryption {
		case domain.BLOB_ENCRYPTION_AES:
			content, err = r.dbAes.Decrypt(content)
			if err != nil {
				wlog.Error().Err(err).Uint64("fid", fid).Msg("decrypt blob error")
				continue
			}
		}
		switch blob.Compression {
		case domain.BLOB_COMPRESSION_GZIP:
			content, err = utils.GUnzipData(content)
			if err != nil {
				wlog.Error().Err(err).Uint64("fid", fid).Msg("unzip blob error")
				continue
			}
		}
		var list protobuf.BlobMessageList
		err = proto.Unmarshal(blob.Messages, &list)
		if err != nil {
			wlog.Error().Str("enc", r.opts.B2.Compression.Encryption.String()).Err(err).Uint64("fid", fid).Msg("unmarshal blob messages list error")
			continue
		}
		list.Fid = blob.Fid
		wlog.Debug().Uint64("fid", list.Fid).Msg("trying send to blob list chan")
		r.blobListChan <- &list
		wlog.Debug().Int("msg count", len(list.Messages)).Uint64("fid", list.Fid).Msg("success sending to blob list chan")
		return nil
	}
}

func (r *ReaderService) sorter(ctx context.Context) {
	wlog := r.logger.With().Int("sort-worker", 0).Logger()
	wlog.Debug().Msg("start sort")
	waitMap := map[uint64]*protobuf.BlobMessage{}
	mu := sync.RWMutex{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sortId := <-r.lastId.C():
				mu.Lock()
				r.bufferChan <- waitMap[sortId]
				delete(waitMap, sortId)
				mu.Unlock()
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case list := <-r.blobListChan:
			wlog.Info().Int("wait map", len(waitMap)).Uint64("current-fid", list.Fid).Uint64("last-id", r.lastId.Current()).Msg("receive sort data")
			for _, msg := range list.Messages {
				mu.Lock()
				waitMap[msg.Id] = msg
				mu.Unlock()
				r.lastId.Add(msg.Id)
			}
		}
	}
}

func (r *ReaderService) b2Sorter(ctx context.Context) {
	wlog := r.logger.With().Int("b2-sort-worker", 0).Logger()
	wlog.Debug().Msg("start sort")
	waitMap := map[uint64]*protobuf.BlobMessage{}
	mu := sync.RWMutex{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sortId := <-r.lastId.C():
				mu.RLock()
				r.bufferChan <- waitMap[sortId]
				mu.RUnlock()
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case list := <-r.blobListChan:
			wlog.Info().Int("wait map", len(waitMap)).Uint64("current-fid", list.Fid).Uint64("last-id", r.lastId.Current()).Msg("receive sort data")
			for _, msg := range list.Messages {
				mu.Lock()
				waitMap[msg.Id] = msg
				mu.Unlock()
				r.lastId.Add(msg.Id)
			}
		}
	}
}

func (r *ReaderService) outer(index int) {
	wlog := r.logger.With().Int("reader-out-worker", index).Logger()
	wlog.Debug().Msg("start reader outer")
	for {
		select {
		case <-r.ctx.Done():
			return
		case m := <-r.bufferChan:
			if m == nil {
				continue
			}
			wlog.Debug().Uint64("id", m.Id).Msg("receive from buffer chan")
			holder := &messageHolder{
				id:      m.Id,
				fid:     m.Fid,
				message: m.Message,
				ack:     make(chan blobAck, 1),
			}
			for {
				r.outChan <- holder
				result := <-holder.ack
				if result.err != nil {
					wlog.Error().Err(result.err).Uint64("id", holder.id).Msg("retry message")
					time.Sleep(500 * time.Millisecond)
				} else {
					break
				}
			}
			wlog.Info().Uint64("last-id", m.Id).Msg("set last-id")
			r.counters.Set(m.Id)
		}
	}
}

func (r *ReaderService) createBucket(bucketName string) (*backblaze.Bucket, error) {
	b2, err := backblaze.NewB2(r.opts.B2.Credentials)
	if err != nil {
		return nil, errors.New("fail to create B2: " + err.Error())
	}
	r.logger.Info().Interface("creds", r.opts.B2.Credentials).Msg("authorize B2")
	if err = b2.AuthorizeAccount(); err != nil {
		return nil, errors.New("fail to authorize B2: " + err.Error())
	}

	bucket, err := b2.Bucket(bucketName)
	if err != nil || bucket == nil {
		bucket, err = b2.CreateBucket(bucketName, backblaze.AllPublic)
		r.logger.Info().Msg("create bucket")
		if err != nil {
			return nil, err
		}
	} else {
		r.logger.Info().Bool("has-bucket", bucket != nil).Msg("connected to bucket")
	}

	return bucket, nil
}

func (r *ReaderService) countPerformance() {
	prevLastId := r.lastId.Current()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.NewTicker(time.Second).C:
			r.performance = r.lastId.Current() - prevLastId
			prevLastId = r.lastId.Current()
		}
	}
}

type messageHolder struct {
	id      uint64
	fid     uint64
	message []byte
	ack     chan blobAck
}

type blobAck struct {
	err error
	id  uint64
}

func (b *messageHolder) Id() uint64 {
	return b.id
}

func (b *messageHolder) Message() []byte {
	return b.message
}

func (b *messageHolder) Done() {
	b.ack <- blobAck{id: b.id}
}

func (b *messageHolder) Error(err error) {
	b.ack <- blobAck{id: b.id, err: err}
}
func (b *messageHolder) UnmarshalProto(v proto.Message) error {
	return proto.Unmarshal(b.message, v)
}
