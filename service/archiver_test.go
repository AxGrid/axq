package service

import (
	"context"
	"github.com/axgrid/axq/domain"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"gopkg.in/kothar/go-backblaze.v0"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewArchiverService_bench(t *testing.T) {
	s, err := NewArchiverService(domain.ArchiverOptions{
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
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
			Compression: domain.CompressionOptions{
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		ChunkSize: 1000,
		MaxSize:   1_000_000,
		MaxCount:  100_000,
	})
	assert.Nil(t, err)

	var count, prev uint64 = 0, 0
	timeout := 160
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				log.Info().Int64("op-per-sec", int64(count-prev)).Int("outer", len(s.outChan)).Msg("tick")
				prev = count
			}
		}
	}()

	go func() {
		for {
			m := <-s.sortChan
			inc := m.ToId - m.FromId
			atomic.AddUint64(&count, inc)
		}
	}()

	<-ctx.Done()
}
