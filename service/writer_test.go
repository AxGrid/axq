/*
 * Created by Zed 05.12.2023, 22:05
 */

package service

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriterService_Push(t *testing.T) {
	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "lima-test",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB:          testDataBase,
			Compression: domain.CompressionOptions{},
		},
		MaxBlobSize: 10000,
	}

	w, err := NewWriterService(opts)
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 20000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err = w.Push([]byte(fmt.Sprintf("test_%d", i)))
			if !assert.Nil(t, err) {
				log.Error().Err(err).Msg("push error")
				panic(err)
			}
		}(i)
	}
	wg.Wait()
	assert.True(t, true)
	fid, lastId, err := w.LastID()
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}
	assert.Equal(t, fid, w.fid)
	assert.Equal(t, uint64(20000), lastId)
}

func TestWriterService_benchmark(t *testing.T) {
	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "lima-test",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB:          testDataBase,
			Compression: domain.CompressionOptions{},
		},
		MaxBlobSize: 10000,
	}

	w, err := NewWriterService(opts)
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}

	count := uint64(0)
	timeout := 10
	ctxt, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	go func() {
		prev := uint64(0)
		for {
			select {
			case <-ctxt.Done():
				return
			case <-time.After(time.Second * 1):
				log.Info().Int64("op-per-sec", int64(count-prev)).Msg("tick")
				prev = count
			}
		}
	}()

	for i := 0; i < 50; i++ {
		go func() {
			for {
				err = w.Push([]byte(fmt.Sprintf("test_%d", i)))
				if !assert.Nil(t, err) {
					log.Error().Err(err).Msg("push error")
					panic(err)
				}
				atomic.AddUint64(&count, 1)
			}
		}()
	}

	<-ctxt.Done()
}

func TestWriterService_partition(t *testing.T) {
	db := testDataBase

	if err := db.Table("axq_test_partition").Set("gorm:table_options", "ENGINE=InnoDB").Set("gorm:table_options", "PARTITION BY KEY (fid) PARTITIONS 4").AutoMigrate(domain.Blob{}); err != nil {
		panic(err)
	}

	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test_partition",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB:          db,
			Compression: domain.CompressionOptions{},
		},
		CreateQueueSize: 1000,
		MaxBlobSize:     100000,
		ChunkSize:       1000,
	}

	w, err := NewWriterService(opts)
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}

	count := uint64(0)
	timeout := 10
	ctxt, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	go func() {
		prev := uint64(0)
		for {
			select {
			case <-ctxt.Done():
				return
			case <-time.After(time.Second * 1):
				log.Info().Int64("op-per-sec", int64(count-prev)).Msg("tick")
				prev = count
			}
		}
	}()

	for i := 0; i < 10000; i++ {
		go func(i int) {
			for {
				err = w.Push([]byte(fmt.Sprintf("test_%d", i)))
				if !assert.Nil(t, err) {
					log.Error().Err(err).Msg("push error")
					panic(err)
				}
				atomic.AddUint64(&count, 1)
			}
		}(i)
	}

	<-ctxt.Done()
}
