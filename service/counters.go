/*
 * Created by Zed 05.12.2023, 21:15
 */

package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/axgrid/axq/domain"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type CounterService struct {
	db               *gorm.DB
	logger           zerolog.Logger
	ctx              context.Context
	name, readerName string
	mu               sync.RWMutex
	lastId           domain.MessageIDs
	lastIdChan       chan domain.MessageIDs
	commitChan       chan domain.MessageIDs
}

func NewCounterService(name, readerName string, ctx context.Context, logger zerolog.Logger, db *gorm.DB, startFromEnd, startFromEndEveryTime, fromLatest bool) (*CounterService, error) {
	r := &CounterService{
		ctx:        ctx,
		logger:     logger,
		name:       name,
		readerName: readerName,
		db:         db,
		lastIdChan: make(chan domain.MessageIDs, 10000),
		commitChan: make(chan domain.MessageIDs, 1000),
	}
	if err := db.AutoMigrate(domain.BlobCounter{}); err != nil {
		return nil, err
	}
	var err error
	if r.lastId, err = r.Get(); err != nil {
		return nil, err
	}
	if r.lastId.Id == 0 || startFromEndEveryTime {
		if startFromEnd {
			blob, err := r.lastBlob(name)
			if err != nil {
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, err
				}
			}
			if err = r.createCounter(blob.FID, blob.ToId); err != nil {
				return nil, err
			}
			r.lastId = domain.MessageIDs{
				FID: blob.FID,
				Id:  blob.ToId,
			}
		} else {
			err = r.createCounter(0, 0)
			if err != nil {
				return nil, err
			}
		}
	}
	if fromLatest {
		blob, err := r.lastBlob(name)
		if err != nil {
			return nil, err
		}
		r.lastId = domain.MessageIDs{
			FID: blob.FID,
			Id:  blob.ToId,
		}
	}
	// todo startMin
	// получаем текущий каунтер
	// получаем минимальный блоб
	// если блоб > мин каунтера
	// мин каунтер = блоб
	go r.set()
	go r.save()
	return r, nil
}

func (r *CounterService) createCounter(fid, id uint64) error {
	return r.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.BlobCounter{
		ReaderName: r.readerName,
		Name:       r.name,
		Fid:        fid,
		ID:         id,
	}).Error
}

func (r *CounterService) Get() (domain.MessageIDs, error) {
	var counter domain.BlobCounter
	err := r.db.Where("reader_name = ? AND name = ?", r.readerName, r.name).First(&counter).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.MessageIDs{}, nil
		}
		return domain.MessageIDs{}, err
	}
	return domain.MessageIDs{
		FID: counter.Fid,
		Id:  counter.ID,
	}, nil
}

// Last возвращает текущую позицию счётчика. Читать поле lastId напрямую нельзя:
// его пишет горутина set.
func (r *CounterService) Last() domain.MessageIDs {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastId
}

func (r *CounterService) setLast(ids domain.MessageIDs) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastId = ids
}

// Set отмечает обработанным одно сообщение. Порядок вызовов произвольный:
// счётчик сдвигается только по непрерывной цепочке, разрывы ждут в pending.
func (r *CounterService) Set(id domain.MessageIDs) {
	r.lastIdChan <- id
}

// Commit сдвигает счётчик сразу на конец непрерывного диапазона. Вызывающий
// обязан гарантировать непрерывность и порядок (архивер коммитит блобы строго
// по возрастанию fid), поэтому цепочка по одному id тут не собирается.
func (r *CounterService) Commit(ids domain.MessageIDs) {
	r.commitChan <- ids
}

func (r *CounterService) set() {
	pending := make(map[uint64]domain.MessageIDs)
	for {
		select {
		case <-r.ctx.Done():
			return
		case ids := <-r.commitChan:
			if ids.Id > r.Last().Id {
				r.setLast(ids)
			}
			r.drainPending(pending)
		case ids := <-r.lastIdChan:
			r.advance(ids, pending)
			for i := len(r.lastIdChan); i > 0; i-- {
				r.advance(<-r.lastIdChan, pending)
			}
		}
	}
}

func (r *CounterService) advance(ids domain.MessageIDs, pending map[uint64]domain.MessageIDs) {
	if ids.Id <= r.Last().Id {
		return
	}
	pending[ids.Id] = ids
	r.drainPending(pending)
}

// drainPending сдвигает счётчик по всем id, которые уже сложились в непрерывную
// цепочку от текущей позиции.
func (r *CounterService) drainPending(pending map[uint64]domain.MessageIDs) {
	for {
		next, ok := pending[r.Last().Id+1]
		if !ok {
			return
		}
		delete(pending, next.Id)
		r.setLast(next)
	}
}

func (r *CounterService) save() {
	var written uint64 = 0
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(3 * time.Second):
			last := r.Last()
			if last.Id > written {
				for {
					if err := r.saveData(last); err != nil {
						r.logger.Error().Err(err).Msg("fail save counter")
						time.Sleep(time.Millisecond * 100)
					} else {
						written = last.Id
						break
					}
				}
			}
		}
	}
}

func (r *CounterService) saveData(ids domain.MessageIDs) error {
	return r.db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", r.readerName, r.name).Updates(map[string]interface{}{
		"id":  ids.Id,
		"fid": ids.FID,
	}).Error
}

func (r *CounterService) lastBlob(name string) (domain.Blob, error) {
	var blob domain.Blob
	if err := r.db.Table(fmt.Sprintf("axq_%s", name)).Order("fid desc").First(&blob).Error; err != nil {
		return blob, err
	}
	return blob, nil
}
