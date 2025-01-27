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
	lastId           uint64
	lastIdChan       chan uint64
}

func NewCounterService(name, readerName string, ctx context.Context, logger zerolog.Logger, db *gorm.DB) (*CounterService, error) {
	r := &CounterService{
		ctx:        ctx,
		logger:     logger,
		name:       name,
		readerName: readerName,
		db:         db,
		lastIdChan: make(chan uint64, 1000),
	}
	if err := db.AutoMigrate(domain.BlobCounter{}); err != nil {
		return nil, err
	}
	var err error
	if r.lastId, err = r.Get(); err != nil {
		return nil, err
	}
	if r.lastId == 0 {
		err = r.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.BlobCounter{
			ReaderName: r.readerName,
			Name:       r.name,
			ID:         r.lastId,
		}).Error
		if err != nil {
			return nil, err
		}
	}

	go r.set()
	go r.save()
	return r, nil
}

func (r *CounterService) Get() (uint64, error) {
	var counter domain.BlobCounter
	err := r.db.Where("reader_name = ? AND name = ?", r.readerName, r.name).First(&counter).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return counter.ID, nil
}

func (r *CounterService) Set(id uint64) {
	r.lastIdChan <- id
}

func (r *CounterService) LastId() uint64 {
	return r.lastId
}

func (r *CounterService) set() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case lastId := <-r.lastIdChan:
			if lastId == r.lastId {
				continue
			}
			for {
				if lastId == r.lastId+1 {
					r.lastId = lastId
				} else {
					r.lastIdChan <- lastId
					break
				}
			}
		}
	}
}

func (r *CounterService) save() {
	var written uint64 = 0
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(3 * time.Second):
			if r.lastId > written {
				for {
					if err := r.saveData(r.lastId); err != nil {
						r.logger.Error().Err(err).Msg("fail save counter")
						time.Sleep(time.Millisecond * 100)
					} else {
						written = r.lastId
						break
					}
				}
			}
		}
	}
}

func (r *CounterService) saveData(id uint64) error {
	return r.db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", r.readerName, r.name).Update("id", id).Error
}
