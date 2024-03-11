package service

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriterService_consistency(t *testing.T) {
	count := 500000
	db := testDataBase
	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB:          db,
			Compression: domain.CompressionOptions{},
		},
		PartitionsCount: 4,
		MaxBlobSize:     100000,
	}

	w, err := NewWriterService(opts)
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < count; i++ {
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

	var fid, lastId uint64 = 1, 0
	for {
		var b domain.Blob
		err = db.Table("axq_test").Where("fid = ?", fid).First(&b).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return
			} else {
				panic(err)
			}
		}
		if lastId+1 != b.FromId {
			panic("invalid from id")
		}
		lastId = b.ToId
		fid++
	}
}

func TestReaderService_consistency_2_waiter_1_outer(t *testing.T) {
	waiters := 2
	db := testDataBase
	ctx := context.Background()
	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		DB: domain.DataBaseOptions{
			DB: db,
			Compression: domain.CompressionOptions{
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize:  1000000,
		BatchSize:   50,
		WaiterCount: waiters,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	ctxt, _ := context.WithTimeout(ctx, time.Second*10)
	elemMap := make(map[uint64]int)
	var fid uint64 = 0
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			curFid := atomic.AddUint64(&fid, 1)
			msg := make([]byte, 8)
			binary.LittleEndian.PutUint64(msg, uint64(index))

			list := &protobuf.BlobMessageList{
				Fid: curFid,
				Messages: []*protobuf.BlobMessage{
					{
						Id:      curFid,
						Fid:     curFid,
						Message: msg,
					},
				},
			}
			r.blobListChan <- list
			mu.Lock()
			elemMap[fid] = 0
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	go func() {
		for {
			m := r.Pop()
			m.Done()
			mu.Lock()
			elemMap[m.Id()] = elemMap[m.Id()] + 1
			mu.Unlock()
		}
	}()
	<-ctxt.Done()

	for _, el := range elemMap {
		if el != 1 {
			panic("wrong reading")
		}
	}
}

func TestReaderService_consistency_10_waiter_5_outer(t *testing.T) {
	ctx := context.Background()
	waiter := 10
	outer := 5
	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
			Compression: domain.CompressionOptions{
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize:  1000000,
		BatchSize:   50,
		WaiterCount: waiter,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	ctxt, _ := context.WithTimeout(ctx, time.Second*10)
	elemMap := make(map[uint64]int)
	var fid uint64 = 0
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			curFid := atomic.AddUint64(&fid, 1)
			msg := make([]byte, 8)
			binary.LittleEndian.PutUint64(msg, uint64(index))

			list := &protobuf.BlobMessageList{
				Fid: curFid,
				Messages: []*protobuf.BlobMessage{
					{
						Id:      curFid,
						Fid:     curFid,
						Message: msg,
					},
				},
			}
			r.blobListChan <- list
			mu.Lock()
			elemMap[curFid] = 0
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for i := 0; i < outer; i++ {
		go func() {
			for {
				m := r.Pop()
				m.Done()
				mu.Lock()
				elemMap[m.Id()] = elemMap[m.Id()] + 1
				mu.Unlock()
			}
		}()
	}

	<-ctxt.Done()

	for k, v := range elemMap {
		if v != 1 {
			fmt.Println(k, v)
			panic("wrong reading")
		}
	}
}

func TestReaderService_consistency_5_waiter_10_outer(t *testing.T) {
	ctx := context.Background()
	waiter := 5
	outer := 10
	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
			Compression: domain.CompressionOptions{
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize:  1000000,
		BatchSize:   50,
		WaiterCount: waiter,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	ctxt, _ := context.WithTimeout(ctx, time.Second*10)
	elemMap := make(map[uint64]int)
	var fid uint64 = 0
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			curFid := atomic.AddUint64(&fid, 1)
			msg := make([]byte, 8)
			binary.LittleEndian.PutUint64(msg, uint64(index))

			list := &protobuf.BlobMessageList{
				Fid: curFid,
				Messages: []*protobuf.BlobMessage{
					{
						Id:      curFid,
						Fid:     curFid,
						Message: msg,
					},
				},
			}
			r.blobListChan <- list
			mu.Lock()
			elemMap[fid] = 0
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for i := 0; i < outer; i++ {
		go func() {
			for {
				m := r.Pop()
				m.Done()
				mu.Lock()
				elemMap[m.Id()] = elemMap[m.Id()] + 1
				mu.Unlock()
			}
		}()
	}

	<-ctxt.Done()

	for _, el := range elemMap {
		if el != 1 {
			panic("wrong reading")
		}
	}
}

func TestB2ReaderService_consistency_2_loader_1_outer(t *testing.T) {
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
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		ReaderName:  "b2_reader",
		LoaderCount: loaders,
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	assert.NotNil(t, r)

	ctxt, _ := context.WithTimeout(ctx, time.Second*10)
	mu := sync.Mutex{}
	elemMap := make(map[uint64]int)
	go func() {
		for {
			m := r.Pop()
			m.Done()
			mu.Lock()
			elemMap[m.Id()] = elemMap[m.Id()] + 1
			mu.Unlock()
		}
	}()

	for _, el := range elemMap {
		if el != 1 {
			panic("wrong reading")
		}
	}

	<-ctxt.Done()
}

func TestB2ReaderService_consistency_5_loader_10_outer(t *testing.T) {
	ctx := context.Background()
	loaders := 5
	outers := 10
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
		LoaderCount: loaders,
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	assert.NotNil(t, r)

	ctxt, _ := context.WithTimeout(ctx, time.Second*10)
	mu := sync.Mutex{}
	elemMap := make(map[uint64]int)

	for i := 0; i < outers; i++ {
		go func() {
			for {
				m := r.Pop()
				m.Done()
				mu.Lock()
				elemMap[m.Id()] = elemMap[m.Id()] + 1
				mu.Unlock()
			}
		}()
	}
	<-ctxt.Done()

	for _, el := range elemMap {
		if el != 1 {
			panic("wrong reading")
		}
	}
}

func TestB2ReaderService_consistency_10_loader_5_outer(t *testing.T) {
	ctx := context.Background()
	loaders := 10
	outers := 5
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
		LoaderCount: loaders,
	}

	r, err := NewB2ReaderService(opts)
	assert.Nil(t, err)
	assert.NotNil(t, r)

	ctxt, _ := context.WithTimeout(ctx, time.Second*10)
	mu := sync.Mutex{}
	elemMap := make(map[uint64]int)

	for i := 0; i < outers; i++ {
		go func() {
			for {
				m := r.Pop()
				m.Done()
				mu.Lock()
				elemMap[m.Id()] = elemMap[m.Id()] + 1
				mu.Unlock()
			}
		}()
	}
	<-ctxt.Done()

	for _, el := range elemMap {
		if el != 1 {
			panic("wrong reading")
		}
	}
}

func TestReaderService_consistency_rand_errors(t *testing.T) {
	waiters := 2
	ctx := context.Background()
	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
			Compression: domain.CompressionOptions{
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize:  1000000,
		BatchSize:   50,
		WaiterCount: waiters,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	ctxt, _ := context.WithTimeout(ctx, time.Second*6)
	elemMap := make(map[uint64]int)
	var fid uint64 = 0
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	elemCount := 10000
	for i := 0; i < elemCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			curFid := atomic.AddUint64(&fid, 1)
			msg := make([]byte, 8)
			binary.LittleEndian.PutUint64(msg, uint64(index))

			list := &protobuf.BlobMessageList{
				Fid: curFid,
				Messages: []*protobuf.BlobMessage{
					{
						Id:      curFid,
						Fid:     curFid,
						Message: msg,
					},
				},
			}
			r.blobListChan <- list
			mu.Lock()
			elemMap[fid] = 0
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	rnd := rand.New(rand.NewSource(time.Now().UnixMicro()))
	go func() {
		for {
			m := r.Pop()
			rnum := rnd.Int63n(100)
			if rnum > 20 {
				m.Done()
			} else {
				m.Error(errors.New("some test error"))
				continue
			}
			mu.Lock()
			elemMap[m.Id()] = elemMap[m.Id()] + 1
			mu.Unlock()
		}
	}()
	<-ctxt.Done()

	for i := 1; i <= elemCount; i++ {
		el := elemMap[uint64(i)]
		if el != 1 {
			panic("wrong reading")
		}
	}
}

func TestArchiverService_overlap(t *testing.T) {
	var lastB2fid uint64 = 6
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
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		ChunkSize: 1000,
		MaxSize:   1_000_000,
		MaxCount:  100_000,
	})
	assert.Nil(t, err)
	assert.NotNil(t, s)

	ctxt, _ := context.WithTimeout(context.Background(), time.Second*5)
	<-ctxt.Done()

	b2Reader, err := NewB2ReaderService(domain.B2ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
		},
		LoaderCount: 1,
		OuterCount:  1,
		BufferSize:  1_000_000,
	})
	assert.Nil(t, err)
	<-time.NewTimer(5 * time.Second).C

	if b2Reader.b2Fid >= lastB2fid {
		panic("error overlap writing")
	}
}

func TestWriterAndArchiver_write(t *testing.T) {
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
		MaxBlobSize: 10000,
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

	_, err = NewArchiverService(domain.ArchiverOptions{
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
				Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
				Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		ChunkSize: 1000,
		MaxSize:   1_000_000,
		MaxCount:  100_000,
	})
	time.Sleep(100 * time.Second)
}

func TestOverlap_small_data(t *testing.T) {
	base := domain.BaseOptions{
		Name:   "test",
		Logger: log.Logger,
		CTX:    context.Background(),
	}
	b2Opts := domain.B2Options{
		Endpoint: b2Endpoint,
		Credentials: backblaze.Credentials{
			KeyID:          b2KeyId,
			ApplicationKey: b2AppKey,
		},
		Salt: "test_salt",
	}
	db := testDataBase
	dbOpts := domain.DataBaseOptions{
		DB: db,
		Compression: domain.CompressionOptions{
			Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
			Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
			EncryptionKey: []byte("12345678901234567890123456789012"),
		},
	}

	//Обнуление таблицы
	err := db.Exec("DELETE FROM `axq_lima-test`").Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "b2arch_test", "test").Update("id", 0).Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "reader_test", "test").Update("id", 0).Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "b2arch_fid", "test").Update("id", 0).Error
	assert.Nil(t, err)

	// Запись 10к строк
	lines := 10000
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
		MaxBlobSize: 10000,
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

	//Запись всей базы в б2
	ar, err := NewArchiverService(domain.ArchiverOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
			Salt: "test_salt",
		},
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
		ChunkSize: 1000,
		MaxSize:   1000,
		MaxCount:  100_000,
	})
	assert.Nil(t, err)
	assert.NotNil(t, ar)
	time.Sleep(8 * time.Second)
	ar.Close()

	// Удаляем первые 5к
	err = db.Table("axq_lima-test").Where("from_id < 5000").Delete(&domain.Blob{}).Error
	assert.Nil(t, err)

	//Запись 5к новых строк
	lines = 5000
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

	//Вычитывание всех данных
	reader, err := NewReaderService(domain.ReaderOptions{
		BaseOptions: base,
		B2:          b2Opts,
		DB:          dbOpts,
		ReaderName:  "reader_test",
		LoaderCount: 2,
		WaiterCount: 2,
		BatchSize:   50,
		BufferSize:  100_000,
	})
	assert.Nil(t, err)
	assert.NotNil(t, reader)

	shouldExistMap := map[uint64]bool{}
	for i := 1; i <= 15000; i++ {
		shouldExistMap[uint64(i)] = false
	}

	uniqueMap := map[uint64]int{}
	go func() {
		for {
			m := reader.Pop()
			m.Done()
			uniqueMap[m.Id()] = uniqueMap[m.Id()] + 1
		}
	}()
	time.Sleep(10 * time.Second)
	for k, v := range uniqueMap {
		shouldExistMap[k] = true
		if v != 1 {
			fmt.Println(k, v)
			panic("incorrect reading")
		}
	}

	for k, v := range shouldExistMap {
		if !v {
			fmt.Println(k, v)
			panic("err")
		}
	}
}

func TestOverlap(t *testing.T) {
	base := domain.BaseOptions{
		Name:   "test",
		Logger: log.Logger,
		CTX:    context.Background(),
	}
	b2Opts := domain.B2Options{
		Endpoint: b2Endpoint,
		Credentials: backblaze.Credentials{
			KeyID:          b2KeyId,
			ApplicationKey: b2AppKey,
		},
		Salt: "test_salt",
	}
	db := testDataBase
	dbOpts := domain.DataBaseOptions{
		DB: db,
		Compression: domain.CompressionOptions{
			Compression:   domain.BlobCompression(protobuf.BlobCompression_BLOB_COMPRESSION_GZIP),
			Encryption:    domain.BlobEncryption(protobuf.BlobEncryption_BLOB_ENCRYPTION_AES),
			EncryptionKey: []byte("12345678901234567890123456789012"),
		},
	}

	//Обнуление таблицы
	err := db.Exec("DELETE FROM `axq_lima-test`").Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "b2arch_test", "test").Update("id", 0).Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "reader_test", "test").Update("id", 0).Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "b2arch_fid", "test").Update("id", 0).Error
	assert.Nil(t, err)

	// Запись 1rк строк
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
		MaxBlobSize: 10000,
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

	//Запись всей базы в б2
	ar, err := NewArchiverService(domain.ArchiverOptions{
		B2: domain.B2Options{
			Endpoint: b2Endpoint,
			Credentials: backblaze.Credentials{
				KeyID:          b2KeyId,
				ApplicationKey: b2AppKey,
			},
			Salt: "test_salt",
		},
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
		ChunkSize: 1000,
		MaxSize:   10_000,
		MaxCount:  100_000,
	})
	assert.Nil(t, err)
	assert.NotNil(t, ar)
	time.Sleep(200 * time.Second)
	ar.Close()

	// Удаляем первые 500к
	err = db.Table("axq_lima-test").Where("from_id < 500000").Delete(&domain.Blob{}).Error
	assert.Nil(t, err)

	//Запись 500к новых строк
	lines = 500000
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

	//Вычитывание всех данных
	reader, err := NewReaderService(domain.ReaderOptions{
		BaseOptions: base,
		B2:          b2Opts,
		DB:          dbOpts,
		ReaderName:  "reader_test",
		LoaderCount: 2,
		WaiterCount: 2,
		BatchSize:   50,
		BufferSize:  100_000,
	})
	assert.Nil(t, err)
	assert.NotNil(t, reader)

	shouldExistMap := map[uint64]bool{}
	for i := 1; i <= 1500000; i++ {
		shouldExistMap[uint64(i)] = false
	}

	uniqueMap := map[uint64]int{}
	go func() {
		for {
			m := reader.Pop()
			m.Done()
			uniqueMap[m.Id()] = uniqueMap[m.Id()] + 1
		}
	}()
	time.Sleep(200 * time.Second)
	for k, v := range uniqueMap {
		shouldExistMap[k] = true
		if v != 1 {
			fmt.Println(k, v)
			panic("incorrect reading")
		}
	}

	for k, v := range shouldExistMap {
		if !v {
			fmt.Println(k, v)
			panic("err")
		}
	}
}
