package service

import (
	"context"
	"errors"
	"fmt"
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
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName: "b2_reader",
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	fmt.Println(r)
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
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName: "b2_reader",
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

func BenchmarkB2ReaderService_loader(b *testing.B) {
	ctx := context.Background()
	opts := domain.B2ReaderOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName: "b2_reader",
	}

	r, err := NewB2ReaderService(opts)
	r.blobListChan = make(chan *protobuf.BlobMessageList, 1000)
	assert.Nil(b, err)
	<-time.NewTimer(5 * time.Second).C
	var first uint64 = 1
	count := 0
	for {
		select {
		case <-time.NewTimer(5 * time.Second).C:
			break
		default:
			if err := r.load(0); err != nil {
				atomic.SwapUint64(&r.b2Fid, first)
				continue
			}
			count++
		}
		break
	}
}

func BenchmarkB2ReaderService(b *testing.B) {
	ctx := context.Background()
	loaders := 30
	opts := domain.B2ReaderOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
		},
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName:  "b2_reader",
		BufferSize:  1_000_000,
		LoaderCount: loaders,
		OuterCount:  loaders * 2,
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(b, err)

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

	for i := 0; i < loaders; i++ {
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
