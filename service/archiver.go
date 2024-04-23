/*
 * Created by Zed 06.12.2023, 19:34
 */

package service

import (
	"bytes"
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/go-errors/errors"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/speps/go-hashids"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
	"sort"
	"sync/atomic"
	"time"
)

type ArchiverService struct {
	opts        domain.ArchiverOptions
	db          *gorm.DB
	logger      zerolog.Logger
	counter     *CounterService
	fidCounter  *CounterService
	tableName   string
	dbAes       *utils.AES
	b2Aes       *utils.AES
	b2Bucket    *backblaze.Bucket
	b2Auth      string
	hashId      *hashids.HashID
	reader      *ReaderService
	ctx         context.Context
	cancelFn    context.CancelFunc
	outChan     chan *protobuf.Blob
	currentBlob *protobuf.Blob
	sortChan    chan messageIds
	sortedIds   []messageIds

	messageList          *protobuf.BlobMessageList
	b2Fid                uint64
	lastDeprecateDeleted uint64
}

func NewArchiverService(opts domain.ArchiverOptions) (*ArchiverService, error) {
	ctx, cancelFn := context.WithCancel(opts.CTX)
	r := &ArchiverService{
		opts:     opts,
		logger:   opts.Logger.With().Str("name", opts.Name).Logger(),
		db:       opts.DB.DB,
		ctx:      ctx,
		cancelFn: cancelFn,
		sortChan: make(chan messageIds, 1),
		outChan:  make(chan *protobuf.Blob, 1),
	}

	if opts.B2.Endpoint == "" {
		return nil, errors.New("b2 endpoint empty")
	}

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

	var err error
	r.b2Auth, err = utils.AuthorizeB2(opts.B2.Credentials)
	if err != nil {
		return nil, err
	}
	go r.authorizeB2()

	r.b2Bucket, err = r.createBucket("axqueue-" + utils.GetMD5Hash([]byte(opts.Name), opts.B2.Salt))
	if err != nil {
		return nil, err
	}
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
	archiverName := fmt.Sprintf("b2arch_%s", opts.Name)
	r.counter, err = NewCounterService(opts.Name, archiverName, opts.CTX, opts.Logger, r.db)
	if err != nil {
		return nil, err
	}

	id, err := r.counter.Get()
	if err != nil {
		return nil, err
	}
	r.fidCounter, err = NewCounterService(fmt.Sprintf("%s_fid", opts.Name), archiverName, opts.CTX, opts.Logger, r.db)
	if err != nil {
		return nil, err
	}
	fid, err := r.fidCounter.Get()
	if err != nil {
		return nil, err
	}
	if fid > 0 {
		r.b2Fid = fid
	}
	readerName := fmt.Sprintf("reader_%s", opts.Name)
	r.reader, err = NewReaderService(domain.ReaderOptions{
		BaseOptions: opts.BaseOptions,
		ReaderName:  readerName,
		DB:          opts.DB,
		B2Bucket:    r.b2Bucket,
		LoaderCount: opts.Reader.LoaderCount,
		WaiterCount: opts.Reader.WaiterCount,
		BufferSize:  5_000_000,
		BatchSize:   50,
		LastId: &domain.LastIdOptions{
			LastId: id,
		},
	})
	if err != nil {
		return nil, err
	}
	go r.loader(0)
	go r.sorter()
	go r.cleaner()
	for i := 0; i < opts.OuterCount; i++ {
		go r.outer(i)
	}

	return r, nil
}

func (a *ArchiverService) loader(index int) {
	wlog := a.logger.With().Int("archiver loader", index).Logger()
	wlog.Debug().Msg("start archiver loader")
	for {
		select {
		case <-a.ctx.Done():
			return
		case msg := <-a.reader.C():
			if msg.Id() <= a.counter.lastId {
				continue
			}
			if msg.Id()%1000 == 0 {
				wlog.Info().Msgf("reader %d", msg.Id())
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
			}
			a.currentBlob.ToId = msg.Id()
			a.messageList.Messages = append(a.messageList.Messages, &protobuf.BlobMessage{
				Id:      msg.Id(),
				Message: msg.Message(),
			})
			msg.Done()

			if len(a.messageList.Messages)%a.opts.ChunkSize == 0 {
				size, err := a.calculateBlobSize()
				if err != nil {
					wlog.Error().Err(err).Msg("fail calculate blob size")
					continue
				}
				if size > int64(a.opts.MaxSize) || len(a.messageList.Messages) > a.opts.MaxCount {
					a.outChan <- a.currentBlob
					wlog.Info().Int64("size", size).Msgf("send blob %d", a.currentBlob.Fid)
					a.currentBlob = nil
					a.messageList = nil
					continue
				}
			}
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
			blobMD5 := utils.GetMD5Hash(bts, "")
			for {
				_, err = a.b2Bucket.UploadFile(filename, metadata, bytes.NewBuffer(bts))
				if err != nil {
					wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail upload blob")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				for i := 0; i < 5; i++ {
					b2File, err := utils.DownloadFileByName(a.b2Bucket.Name, a.opts.B2.Endpoint, filename)
					if err != nil {
						wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to check uploaded file")
						time.Sleep(100 * time.Millisecond)
						continue
					}

					if utils.GetMD5Hash(b2File, "") != blobMD5 {
						wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail to check md5")
						time.Sleep(100 * time.Millisecond)
						continue
					}
				}
				wlog.Info().Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Str("filename", filename).Msg("successfully uploaded")
				break
			}

			a.sortChan <- messageIds{
				FromId: m.FromId,
				ToId:   m.ToId,
			}
			a.fidCounter.Set(m.Fid)
		}
	}
}

type messageIds struct {
	FromId uint64
	ToId   uint64
}

func (a *ArchiverService) sorter() {
	wlog := a.logger.With().Int("archiver-sort-worker", 0).Logger()
	wlog.Debug().Msg("start sorter")
	for {
		select {
		case <-a.ctx.Done():
			return
		case msgs := <-a.sortChan:
			a.sortedIds = append(a.sortedIds, msgs)
			sort.Slice(a.sortedIds, func(i, j int) bool {
				return a.sortedIds[i].FromId < a.sortedIds[j].FromId
			})

			for _, s := range a.sortedIds {
				if s.FromId <= a.counter.lastId {
					continue
				}
				if a.counter.lastId+1 == s.FromId {
					a.counter.lastId = s.ToId
				} else {
					a.counter.Set(s.ToId)
					wlog.Info().Uint64("to-id", s.ToId).Msg("counter set new to-id")
					break
				}
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

func (a *ArchiverService) cleaner() {
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-time.NewTimer(time.Duration(a.opts.CleanTimeout) * time.Second).C:
			a.logger.Info().Uint64("last-id", a.counter.lastId).Msg("cleaner started")
			if a.counter.lastId < a.opts.DeprecatedFrom {
				continue
			}
			deleteTo := a.counter.lastId - a.opts.DeprecatedFrom
			if deleteTo > a.reader.lastId.Current()-a.opts.Intersection {
				deleteTo = a.reader.lastId.Current() - a.opts.Intersection
			}
			a.logger.Info().Uint64("delete from", deleteTo).Msg("deprecate counted")
			if err := a.db.Table(a.tableName).Where("to_id <= ?", deleteTo).Delete(&domain.Blob{}).Error; err != nil {
				a.logger.Error().Err(err).Msg("err while deleting deprecated data")
				continue
			}
			a.logger.Info().Uint64("to-id", deleteTo).Msg("successfully deleted deprecate data")
			a.lastDeprecateDeleted = deleteTo
		}
	}
}

func (a *ArchiverService) authorizeB2() {
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-time.NewTimer(domain.B2TokenTTL * time.Hour).C:
			token, err := utils.AuthorizeB2(a.opts.B2.Credentials)
			if err != nil {
				a.logger.Error().Err(err).Msg("cant update b2 auth token")
				continue
			}
			a.b2Auth = token
		}
	}
}

func (a *ArchiverService) calculateBlobSize() (size int64, err error) {
	a.currentBlob.Messages, err = proto.Marshal(a.messageList)
	if err != nil {
		return
	}
	switch a.opts.B2.Compression.Compression {
	case domain.BLOB_COMPRESSION_GZIP:
		a.currentBlob.Messages, err = utils.GZipData(a.currentBlob.Messages)
		if err != nil {
			return
		}
	}
	switch a.opts.B2.Compression.Encryption {
	case domain.BLOB_ENCRYPTION_AES:
		a.currentBlob.Messages, err = a.b2Aes.Encrypt(a.currentBlob.Messages)
		if err != nil {
			return
		}
	}
	size = int64(len(a.currentBlob.Messages))
	return
}

func (a *ArchiverService) Close() {
	a.cancelFn()
}
