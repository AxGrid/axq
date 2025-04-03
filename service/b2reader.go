package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/speps/go-hashids"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
	"sync/atomic"
	"time"
)

type B2ReaderService struct {
	opts         domain.B2ReaderOptions
	logger       zerolog.Logger
	ctx          context.Context
	cancelFunc   context.CancelFunc
	counters     *CounterService
	db           *gorm.DB
	blobListChan chan *protobuf.BlobMessageList
	outChan      chan domain.Message
	bufferChan   chan *protobuf.BlobMessage
	dbAes        *utils.AES
	fidLock      *FidLock
	b2Fid        uint64
	lastId       uint64
	b2Bucket     *backblaze.Bucket
	hashId       *hashids.HashID
}

func NewB2ReaderService(opts domain.B2ReaderOptions) (*B2ReaderService, error) {
	if opts.ReaderName == "" {
		opts.ReaderName = fmt.Sprintf("%s_%s", opts.Name, utils.UUIDString())
	}
	ctx, cancelFn := context.WithCancel(opts.CTX)
	b2r := &B2ReaderService{
		opts:         opts,
		ctx:          ctx,
		cancelFunc:   cancelFn,
		logger:       opts.Logger,
		blobListChan: make(chan *protobuf.BlobMessageList, opts.LoaderCount*2),
		bufferChan:   make(chan *protobuf.BlobMessage, opts.BufferSize),
		outChan:      make(chan domain.Message, opts.OuterCount),
		db:           opts.DB.DB,
	}

	if opts.B2.Salt != "" {
		hashIdData := hashids.NewData()
		hashIdData.Salt = opts.B2.Salt
		hashIdData.MinLength = 10
		var err error
		b2r.hashId, err = hashids.NewWithData(hashIdData)
		if err != nil {
			return nil, err
		}
	}

	var err error
	b2r.b2Bucket, err = b2r.createBucket("axqueue-" + utils.GetMD5Hash([]byte(opts.Name), opts.B2.Salt))
	if err != nil {
		return nil, err
	}

	b2r.counters, err = NewCounterService(opts.Name, opts.ReaderName, opts.CTX, opts.Logger, b2r.db)
	if err != nil {
		return nil, err
	}

	if opts.LastId != nil {
		b2r.lastId = opts.LastId.LastId
	}
	b2r.fidLock = NewFidLock(b2r.b2Fid)

	for i := 0; i < opts.LoaderCount; i++ {
		go b2r.loader(i)
	}
	go b2r.sorter()
	for i := 0; i < opts.OuterCount; i++ {
		go b2r.outer(i)
	}

	return b2r, nil
}

func (r *B2ReaderService) Pop() domain.Message {
	return <-r.outChan
}

func (r *B2ReaderService) C() <-chan domain.Message {
	return r.outChan
}

func (r *B2ReaderService) loader(index int) {
	wlog := r.logger.With().Int("b2reader-loader-worker", index).Logger()
	wlog.Debug().Msg("start b2 loader")
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			if err := r.load(index); err != nil {
				wlog.Error().Err(err)
			}
		}
	}
}

func (r *B2ReaderService) load(index int) error {
	wlog := r.logger.With().Int("b2reader-loader-worker", index).Logger()
	fid := atomic.AddUint64(&r.b2Fid, 1)
	for {
		filename, err := utils.GetBlobFileName(r.hashId, r.opts.Name, fid)
		if err != nil {
			wlog.Error().Err(err).Uint64("fid", fid).Msg("fail to get filename")
			continue
		}
		downloadStart := time.Now()
		wlog.Debug().Uint64("fid", fid).Str("filename", filename).Msg("try to download file")
		content, err := utils.DownloadFileByName(r.b2Bucket.Name, r.opts.B2.Endpoint, filename)
		if err != nil {
			if errors.Is(domain.ErrB2FileNotFound, err) {
				wlog.Warn().Uint64("fid", fid).Msg("fid not found")
			} else {
				wlog.Error().Err(err).Uint64("fid", fid).Msg("download file error")
			}
			time.Sleep(1 * time.Second)
			continue
		}
		wlog.Info().Int64("download took ms", time.Since(downloadStart).Milliseconds()).Uint64("fid", fid).Str("filename", filename).Msg("successfully downloaded file")
		var blob protobuf.Blob
		err = proto.Unmarshal(content, &blob)
		if err != nil {
			wlog.Error().Str("enc", r.opts.B2.Compression.Encryption.String()).Err(err).Uint64("fid", fid).Msg("unmarshal blob error")
			continue
		}
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
		wlog.Debug().Uint64("fid", list.Fid).Int("msg count", len(list.Messages)).Msg("success sending to blob list chan")
		return nil
	}
}

func (r *B2ReaderService) sorter() {
	wlog := r.logger.With().Int("sort-worker", 0).Logger()
	wlog.Debug().Msg("start sort")
	waitMap := make(map[uint64]*protobuf.BlobMessageList)
	for {
		select {
		case <-r.ctx.Done():
			return
		case list := <-r.blobListChan:
			wlog.Info().Int("map", len(waitMap)).Int("buf", len(r.bufferChan)).Uint64("wait-fid", r.fidLock.fid+1).Uint64("current-fid", list.Fid).Msg("sorted data")
			if r.fidLock.fid+1 != list.Fid {
				waitMap[list.Fid] = list
				continue
			}
			wlog.Debug().Uint64("fid", list.Fid).Int("messages count", len(list.Messages)).Msg("got blob")
			for _, m := range list.Messages {
				if m.Id <= r.lastId {
					continue
				}
				m.Fid = list.Fid
				r.bufferChan <- m
			}
			wlog.Debug().Uint64("last-id", r.lastId).Uint64("fid", list.Fid).Msg("successfully processed blob")
			r.fidLock.fid++
			list, ok := waitMap[r.fidLock.fid+1]
			for ok {
				wlog.Debug().Uint64("fid", list.Fid).Int("messages count", len(list.Messages)).Msg("got blob from map")
				delete(waitMap, r.fidLock.fid)
				for _, m := range list.Messages {
					if m.Id <= r.lastId {
						continue
					}
					m.Fid = list.Fid
					r.bufferChan <- m
				}
				wlog.Debug().Uint64("last-id", r.lastId).Uint64("fid", list.Fid).Msg("successfully processed blob from map")
				r.fidLock.fid++
				list, ok = waitMap[r.fidLock.fid+1]
			}
		}
	}
}

func (r *B2ReaderService) outer(index int) {
	wlog := r.logger.With().Int("out-worker", index).Logger()
	wlog.Debug().Msg("start outer")
	for {
		select {
		case <-r.ctx.Done():
			return
		case m := <-r.bufferChan:
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
				} else {
					break
				}
			}
			//r.counters.Set(m.Id)
		}
	}
}

func (r *B2ReaderService) createBucket(bucketName string) (*backblaze.Bucket, error) {
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
