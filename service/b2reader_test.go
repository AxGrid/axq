package service

import (
	"context"
	"errors"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"gopkg.in/kothar/go-backblaze.v0"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewB2ReaderService(t *testing.T) {
	ctx := context.Background()
	opts := domain.B2ReaderOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
			Salt: "test_salt",
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName:  "b2_reader",
		LoaderCount: 2,
		OuterCount:  2,
	}
	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	assert.NotNil(t, r)
}

func TestB2ReaderService_sorter(t *testing.T) {
	ctx := context.Background()
	loaders := 2
	opts := domain.B2ReaderOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
			Salt: "test_salt",
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName:  "b2_reader",
		LoaderCount: 2,
		OuterCount:  2,
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)

	stuckTimer := time.NewTimer(2 * time.Second)
	for i := 0; i < loaders; i++ {
		go func(i int) {
			for {
				if err = r.load(i); err != nil {
					if errors.Is(err, domain.ErrB2FileNotFound) {
						stuckTimer.Reset(2 * time.Second)
					}
				}
			}
		}(i)
	}

	go r.sorter()

	var fid uint64 = 0
	go func() {
		for {
			select {
			case <-stuckTimer.C:
			case m := <-r.bufferChan:
				stuckTimer.Reset(2 * time.Second)
				if fid == 0 {
					fid = m.Fid
					continue
				}

				if m.Fid < fid || m.Fid > fid+1 {
					panic("invalid fid")
				}
				fid = m.Fid
			}
		}
	}()
	<-time.NewTimer(15 * time.Second).C
}

func TestB2ReaderService_loader(t *testing.T) {
	ctx := context.Background()
	opts := domain.B2ReaderOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
			Salt: "test_salt",
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName:  "b2_reader",
		LoaderCount: 2,
		OuterCount:  2,
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	assert.NotNil(t, r)
	r.blobListChan = make(chan *protobuf.BlobMessageList, 1000)
	ctxt, _ := context.WithTimeout(ctx, 10*time.Second)
	go func() {
		for {
			m := r.Pop()
			m.Done()
		}
	}()
	<-ctxt.Done()
}

func TestB2ReaderService_Benchmark(t *testing.T) {
	ctx := context.Background()
	opts := domain.B2ReaderOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
			Salt: "test_salt",
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName:  "b2_reader",
		LoaderCount: 2,
		OuterCount:  2,
	}
	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	count := uint64(0)
	timeout := 60
	ctxt, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	go func() {
		prev := uint64(0)
		for {
			select {
			case <-ctxt.Done():
				return
			case <-time.After(time.Second):
				log.Info().Int64("op-per-sec", int64(count-prev)).Int("buffer", len(r.bufferChan)).Int("blob list", len(r.blobListChan)).Int("out chan", len(r.outChan)).Msg("tick")
				prev = count
			}
		}
	}()
	for i := 0; i < 4; i++ {
		go func() {
			for {
				m := r.Pop()
				m.Done()
				atomic.AddUint64(&count, 1)
			}
		}()
	}
	<-ctxt.Done()
}
