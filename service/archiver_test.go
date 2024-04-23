package service

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"gopkg.in/kothar/go-backblaze.v0"
	"sync"
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

func TestArchiverService_Cleaner(t *testing.T) {

	utils.InitLogger("info")
	db := testDataBase

	lines := 1000000
	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB: db,
			Compression: domain.CompressionOptions{
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		PartitionsCount: 4,
		MaxBlobSize:     10000,
	}

	w, err := NewWriterService(opts)
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}
	assert.NotNil(t, w)

	wg := sync.WaitGroup{}
	for i := 0; i < lines; i++ {
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

	s, err := NewArchiverService(domain.ArchiverOptions{
		Reader: domain.ReaderOptions{
			LoaderCount: 1,
			WaiterCount: 1,
		},
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
			DB: db,
			Compression: domain.CompressionOptions{
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		ChunkSize:      1000,
		CleanTimeout:   4,
		DeprecatedFrom: 2000,
		Intersection:   1500,
		MaxSize:        1_000_000,
		MaxCount:       100_000,
		OuterCount:     1,
	})
	assert.Nil(t, err)
	assert.NotNil(t, s)

	<-time.NewTimer(10 * time.Second).C
	var res []domain.Blob
	err = db.Table(s.tableName).Where("to_id <= ?", s.lastDeprecateDeleted).Find(&res).Error
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))
}
