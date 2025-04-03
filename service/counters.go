/*
 * Created by Zed 05.12.2023, 21:15
 */

package service

import (
	"context"
	"errors"
	"github.com/axgrid/axq/domain"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type CounterService struct {
	db               *gorm.DB
	logger           zerolog.Logger
	ctx              context.Context
	name, readerName string
	lastId           domain.MessageIDs
	lastIdChan       chan domain.MessageIDs
}

func NewCounterService(name, readerName string, ctx context.Context, logger zerolog.Logger, db *gorm.DB) (*CounterService, error) {
	r := &CounterService{
		ctx:        ctx,
		logger:     logger,
		name:       name,
		readerName: readerName,
		db:         db,
		lastIdChan: make(chan domain.MessageIDs, 10000),
	}
	if err := db.AutoMigrate(domain.BlobCounter{}); err != nil {
		return nil, err
	}
	var err error
	if r.lastId, err = r.Get(); err != nil {
		return nil, err
	}
	if r.lastId.Id == 0 {
		err = r.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.BlobCounter{
			ReaderName: r.readerName,
			Name:       r.name,
			Fid:        r.lastId.FID,
			ID:         r.lastId.Id,
		}).Error
		if err != nil {
			return nil, err
		}
	}

	go r.set()
	go r.save()
	return r, nil
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

func (r *CounterService) Set(id domain.MessageIDs) {
	r.lastIdChan <- id
}

func (r *CounterService) set() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case lastId := <-r.lastIdChan:
			r.compareAndSwapLast(lastId)
			for i := 0; i < len(r.lastIdChan); i++ {
				lastId = <-r.lastIdChan
				r.compareAndSwapLast(lastId)
			}
		}
	}
}

func (r *CounterService) compareAndSwapLast(lastId domain.MessageIDs) {
	if r.lastId.Id == lastId.Id {
		return
	}
	if r.lastId.Id > lastId.Id {
		return
	}
	if r.lastId.Id+1 == lastId.Id {
		r.lastId = lastId
		return
	}
	r.lastIdChan <- lastId
}

func (r *CounterService) save() {
	var written uint64 = 0
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(3 * time.Second):
			if r.lastId.Id > written {
				for {
					if err := r.saveData(r.lastId); err != nil {
						r.logger.Error().Err(err).Msg("fail save counter")
						time.Sleep(time.Millisecond * 100)
					} else {
						written = r.lastId.Id
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
