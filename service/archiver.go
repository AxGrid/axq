/*
 * Created by Zed 06.12.2023, 19:34
 */

package service

import (
	"bytes"
	"context"
	"fmt"
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

type ArchiverService struct {
	opts                 domain.ArchiverOptions
	db                   *gorm.DB
	logger               zerolog.Logger
	tableName            string
	counters             *CounterService
	dbAes                *utils.AES
	b2Aes                *utils.AES
	b2Bucket             *backblaze.Bucket
	hashId               *hashids.HashID
	reader               *ReaderService
	ctx                  context.Context
	cancelFn             context.CancelFunc
	outChan              chan *protobuf.Blob
	currentBlob          *protobuf.Blob
	messageList          *protobuf.BlobMessageList
	blobIdsChan          chan domain.BlobIDs
	b2Fid                uint64
	lastDeprecateDeleted uint64
}

func NewArchiverService(opts domain.ArchiverOptions) (*ArchiverService, error) {
	ctx, cancelFn := context.WithCancel(opts.CTX)
	r := &ArchiverService{
		opts:        opts,
		logger:      opts.Logger.With().Str("name", opts.Name).Logger(),
		db:          opts.DB.DB,
		ctx:         ctx,
		cancelFn:    cancelFn,
		outChan:     make(chan *protobuf.Blob, 1000),
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
	r.b2Bucket, err = r.createBucket(bucketName)
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
	archiverName := fmt.Sprintf("b2_archiver_%s", opts.Name)
	r.counters, err = NewCounterService(opts.Name, archiverName, opts.CTX, opts.Logger, r.db, false) // B2 Counters
	if err != nil {
		return nil, err
	}
	lastId, err := r.counters.Get()
	if err != nil {
		return nil, err
	}
	r.b2Fid = lastId.FID
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
			FID:    0,
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
			fmt.Println(msg.Id(), "READ", a.counters.lastId)
			if msg.Id() <= a.counters.lastId.Id {
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
				if size > int64(a.opts.MaxSize) || len(a.messageList.Messages) >= a.opts.MaxCount {
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
			//blobMD5 := utils.GetMD5Hash(bts, "")
			for {
				_, err = a.b2Bucket.UploadFile(filename, metadata, bytes.NewBuffer(bts))
				if err != nil {
					wlog.Error().Err(err).Uint64("fid", m.Fid).Uint64("from-id", m.FromId).Uint64("to-id", m.ToId).Uint64("total", m.Count).Msg("fail upload blob")
					time.Sleep(100 * time.Millisecond)
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
				a.blobIdsChan <- domain.BlobIDs{
					FID:    m.Fid,
					FromId: m.FromId,
					ToId:   m.ToId,
				}
				break
			}

		}
	}
}

type messageIds struct {
	FID    uint64
	FromId uint64
	ToId   uint64
}

// {FID: 200, FromId: 1000, ToId: 1010}
// {FID: 201, FromId: 1011, ToId: 1020}
func (a *ArchiverService) sorter() {
	wlog := a.logger.With().Int("archiver-sort-worker", 0).Logger()
	wlog.Debug().Msg("start sorter")
	for {
		select {
		case <-a.ctx.Done():
			return
		case ids := <-a.blobIdsChan:
			if a.counters.lastId.Id+1 != ids.FromId {
				fmt.Println("SKIP", ids)
				a.blobIdsChan <- ids
				continue
			}
			fmt.Println("GLOBAL SET", ids)
			for i := ids.FromId; i <= ids.ToId; i++ {
				msgIds := domain.MessageIDs{
					FID: ids.FID,
					Id:  i,
				}
				a.counters.Set(msgIds)
				fmt.Println("ITER SET", ids)
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
