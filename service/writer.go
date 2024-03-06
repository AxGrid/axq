/*
 * Created by Zed 05.12.2023, 21:19
 */

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
	"gorm.io/gorm"
	"time"
)

type dataHolder struct {
	message  []byte
	response chan error
}

type WriterService struct {
	opts           domain.WriterOptions
	fid            uint64
	lastId         uint64
	inChan         chan *dataHolder
	createBlobChan chan blobCreate
	aes            *utils.AES
	logger         zerolog.Logger
	db             *gorm.DB
	tableName      string
	cancelFunc     context.CancelFunc
	ctx            context.Context
	performance    uint64
	stopped        bool
}

func NewWriterService(opts domain.WriterOptions) (*WriterService, error) {
	ctx, cancelFunc := context.WithCancel(opts.CTX)
	w := &WriterService{
		opts:           opts,
		inChan:         make(chan *dataHolder, opts.MaxBlobSize),
		createBlobChan: make(chan blobCreate, opts.CreateQueueSize),
		logger:         opts.Logger.With().Str("name", opts.Name).Logger(),
		db:             opts.DB.DB,
		ctx:            ctx,
		cancelFunc:     cancelFunc,
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
		w.aes = aes
	}
	tableName := fmt.Sprintf("axq_%s", opts.Name)
	if !w.db.Migrator().HasTable(tableName) {
		opts.Logger.Debug().Str("table", tableName).Msg("create table")
		if err := w.db.Table(tableName).AutoMigrate(domain.Blob{}); err != nil {
			return nil, errors.New(fmt.Sprintf("fail migrate table:(%s): %s", tableName, err))
		}
	}
	w.tableName = tableName
	var blob domain.Blob
	if err := w.db.Table(tableName).Order("fid desc").First(&blob).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.logger.Debug().Msgf("table:(%s) is empty", tableName)
			w.lastId = 0
			w.fid = 0
		} else {
			return nil, err
		}
	} else {
		w.lastId = blob.ToId
		w.fid = blob.FID
	}
	go w.save()
	go w.create()
	go w.countPerformance()
	return w, nil
}

func (w *WriterService) Close() {
	w.stopped = true
	for {
		if len(w.inChan) == 0 {
			break
		}
	}
	w.cancelFunc()
}

func (w *WriterService) Push(message []byte) error {
	if w.stopped {
		return errors.New("writer stopped")
	}
	holder := &dataHolder{
		message:  message,
		response: make(chan error, 1),
	}
	w.inChan <- holder
	return <-holder.response
}

func (w *WriterService) LastID() (uint64, uint64, error) {
	var blob domain.Blob
	if err := w.db.Table(w.tableName).Order("fid desc").First(&blob).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, 0, nil
		} else {
			return 0, 0, err
		}
	} else {
		return blob.FID, blob.ToId, nil
	}
}

func (w *WriterService) GetOpts() domain.ServiceOpts {
	return &w.opts
}

func (w *WriterService) GetPerformance() uint64 {
	return w.performance
}

func (w *WriterService) save() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case data := <-w.inChan:
			blobList := make([]*dataHolder, 0, w.opts.MaxBlobSize)
			blobList = append(blobList, data)
			for len(blobList)%w.opts.ChunkSize != 0 {
				data = <-w.inChan
				blobList = append(blobList, data)
			}
			blob, err := w.prepare(blobList)
			if err != nil {
				w.logger.Error().Err(err).Uint64("fid", blob.FID).Uint64("from-id", blob.FromId).Uint64("to-id", blob.ToId).Int("total", blob.Total).Msg("fail prepare blob")
				for _, data = range blobList {
					data.response <- err
				}
				continue
			}

			w.createBlobChan <- blobCreate{
				blob:     blob,
				dataList: blobList,
			}
			w.logger.Debug().Uint64("fid", blob.FID).Msg("sent to create blob chan")
		}
	}
}

func (w *WriterService) prepare(blobList []*dataHolder) (*domain.Blob, error) {
	list := &protobuf.BlobMessageList{
		Messages: make([]*protobuf.BlobMessage, len(blobList)),
	}
	messageLastId := w.lastId
	for i, data := range blobList {
		messageLastId++
		list.Messages[i] = &protobuf.BlobMessage{
			Id:      messageLastId,
			Message: data.message,
		}
	}
	blobBytes, err := proto.Marshal(list)
	if err != nil {
		return nil, err
	}
	switch w.opts.DB.Compression.Compression {
	case domain.BLOB_COMPRESSION_GZIP:
		blobBytes, err = utils.GZipData(blobBytes)
	}
	if err != nil {
		return nil, err
	}
	switch w.opts.DB.Compression.Encryption {
	case domain.BLOB_ENCRYPTION_AES:
		blobBytes, err = w.aes.Encrypt(blobBytes)
	}
	if err != nil {
		return nil, err
	}

	blob := &domain.Blob{
		FID:         w.fid + 1,
		Compression: w.opts.DB.Compression.Compression,
		Encryption:  w.opts.DB.Compression.Encryption,
		Total:       len(list.Messages),
		FromId:      list.Messages[0].Id,
		ToId:        list.Messages[len(list.Messages)-1].Id,
		Message:     blobBytes,
	}
	w.fid = blob.FID
	w.lastId = blob.ToId
	return blob, nil
}

type blobCreate struct {
	blob     *domain.Blob
	dataList []*dataHolder
}

func (w *WriterService) create() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case blobData := <-w.createBlobChan:
			w.logger.Debug().Uint64("fid", blobData.blob.FID).Int("total", len(blobData.dataList)).Msg("get blob")
			err := w.db.Table(w.tableName).Create(blobData.blob).Error
			if err != nil {
				w.logger.Error().Err(err).Int("size", len(blobData.blob.Message)).Uint64("fid", blobData.blob.FID).Uint64("from-id", blobData.blob.FromId).Uint64("to-id", blobData.blob.ToId).Int("total", blobData.blob.Total).Msg("fail create blob")
				for _, data := range blobData.dataList {
					data.response <- err
				}
				continue
			}
			w.logger.Debug().Int("size", len(blobData.blob.Message)).Uint64("fid", blobData.blob.FID).Uint64("from-id", blobData.blob.FromId).Uint64("to-id", blobData.blob.ToId).Int("total", blobData.blob.Total).Msg("create blob")
			for _, data := range blobData.dataList {
				data.response <- nil
			}
		}
	}
}

func (w *WriterService) countPerformance() {
	prevLastId := w.lastId
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-time.NewTicker(time.Second).C:
			w.performance = w.lastId - prevLastId
			prevLastId = w.lastId
		}
	}
}
