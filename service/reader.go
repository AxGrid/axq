/*
 * Created by Zed 06.12.2023, 09:38
 */

package service

import (
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
	"golang.org/x/exp/constraints"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
)

type T constraints.Integer

type ReaderService struct {
	opts           domain.ReaderOptions
	logger         zerolog.Logger
	tableName      string
	counters       *CounterService
	lastId         *utils.MinimalId[uint64]
	sortIdChan     chan uint64
	dbFid          uint64
	b2Fid          uint64
	blobListChan   chan *protobuf.BlobMessageList
	bufferChan     chan *protobuf.BlobMessage
	outChan        chan domain.Message
	db             *gorm.DB
	batchSize      uint64
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
	counters, err := r.counters.Get()
	if err != nil {
		return 0, err
	}
	return counters.Id, nil
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
		opts: opts,
		//logger:       opts.Logger.With().Str("reader name", opts.ReaderName).Logger(),
		logger:       zerolog.Nop(),
		blobListChan: make(chan *protobuf.BlobMessageList, opts.LoaderCount*opts.LoaderCount),
		bufferChan:   make(chan *protobuf.BlobMessage, opts.BufferSize),
		outChan:      make(chan domain.Message, opts.WaiterCount),
		db:           opts.DB.DB,
		batchSize:    opts.BatchSize,
		ctx:          ctx,
		cancelFunc:   cancelFn,
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
	r.counters, err = NewCounterService(opts.Name, opts.ReaderName, opts.CTX, opts.Logger, r.db, opts.StartFromEnd)
	if err != nil {
		return nil, err
	}
	if opts.LastId == nil {
		lastId, err := r.counters.Get()
		if err != nil {
			return nil, err
		}
		r.lastId = utils.NewMinimalId(lastId.Id)
		if r.lastId.Current() > 0 {
			r.dbFid = lastId.FID
		}
	} else {
		r.lastId = utils.NewMinimalId(opts.LastId.LastId)
		if r.lastId.Current() > 0 {
			r.dbFid = opts.LastId.FID
		}
	}
	r.logger.Debug().Uint64("last-id", r.lastId.Current()).Uint64("fid", r.dbFid).Msg("init reader")
	if r.dbFid > 0 {
		r.dbFid--
	}
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
			r.startDBLoaders()
		}
	}

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

func (r *ReaderService) loadDB(index int) error {
	wlog := r.logger.With().Int("db-loader-worker", index).Logger()
	fid := atomic.AddUint64(&r.dbFid, 1)
	var blob domain.Blob
	for {
		err := r.getData(fid, &blob)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			time.Sleep(250 * time.Millisecond)
			continue
		}
		wlog.Debug().Uint64("fid", fid).Uint64("from-id", blob.FromId).Uint64("to-id", blob.ToId).Msg("db blob")
		data := blob.Message
		switch blob.Encryption {
		case domain.BLOB_ENCRYPTION_AES:
			data, err = r.dbAes.Decrypt(data)
			if err != nil {
				continue
			}
		}
		switch blob.Compression {
		case domain.BLOB_COMPRESSION_GZIP:
			data, err = utils.GUnzipData(data)
			if err != nil {
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
		r.blobListChan <- &list
		wlog.Debug().Uint64("blob fid", blob.FID).Int("blob list chan len", len(r.blobListChan)).Msg("sent to blob list chan")

		return nil
	}
}

func (r *ReaderService) getData(fid uint64, res *domain.Blob) error {
	t := time.Now()
	localCtx, cancelFn := context.WithTimeout(r.ctx, time.Second*5)
	defer func() {
		cancelFn()
		atomic.AddInt64(&r.deltaTime, time.Since(t).Milliseconds())
		atomic.AddInt64(&r.deltaTimeCount, 1)
	}()
	return r.db.WithContext(localCtx).Table(r.tableName).Where("fid = ?", fid).First(res).Error
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
				mu.RLock()
				sort, ok := waitMap[sortId]
				mu.RUnlock()
				if !ok {
					r.lastId.Add(sortId)
					continue
				}
				r.bufferChan <- sort
				mu.Lock()
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
			wlog.Debug().Uint64("blob fid", list.Fid).Msg("read from blob list chan")
			for _, msg := range list.Messages {
				mu.Lock()
				msg.Fid = list.Fid
				waitMap[msg.Id] = msg
				mu.Unlock()
				r.lastId.Add(msg.Id)
				wlog.Debug().Uint64("msg id", msg.Id).Uint64("msg blob fid", list.Fid).Msg("sorted msg")
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
			wlog.Debug().Uint64("msg id", m.Id).Uint64("msg fid", m.Fid).Msg("read from buffer chan")
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
					time.Sleep(25 * time.Millisecond)
				} else {
					break
				}
			}
			r.counters.Set(domain.MessageIDs{
				FID: m.Fid,
				Id:  m.Id,
			})
			wlog.Info().Any("counters", r.counters.lastId).Uint64("minimal last id", r.lastId.Current()).Uint64("message id", m.Id).Uint64("message fid", m.Fid).Msg("set last-id")
		}
	}
}

func (r *ReaderService) countPerformance() {
	prevLastId := r.lastId.Current()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(time.Second):
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
