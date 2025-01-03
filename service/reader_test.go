/*
 * Created by Zed 06.12.2023, 15:20
 */

package service

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewReaderService_read(t *testing.T) {

	base := domain.BaseOptions{
		Name:   "test",
		Logger: log.Logger,
		CTX:    context.Background(),
	}
	db := testDataBase
	dbOpts := domain.DataBaseOptions{
		DB: db,
		Compression: domain.CompressionOptions{
			Compression:   domain.BLOB_COMPRESSION_GZIP,
			Encryption:    domain.BLOB_ENCRYPTION_AES,
			EncryptionKey: []byte("12345678901234567890123456789012"),
		},
	}

	//Обнуление таблицы
	err := db.Exec("DELETE FROM `axq_lima-test`").Error
	assert.Nil(t, err)

	//Обнуление last id
	err = db.Model(&domain.BlobCounter{}).Where("reader_name = ? AND name = ?", "reader_test", "test").Update("id", 0).Error
	assert.Nil(t, err)

	// Запись 100к строк
	lines := 100_000
	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "lima-test",
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
		MaxBlobSize:     10000,
		PartitionsCount: 2,
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

	reader, err := NewReaderService(domain.ReaderOptions{
		BaseOptions: base,
		DB:          dbOpts,
		ReaderName:  "reader_test",
		LoaderCount: 2,
		WaiterCount: 2,
		BatchSize:   50,
		BufferSize:  100_000,
	})
	assert.Nil(t, err)
	assert.NotNil(t, reader)

	uniqueMap := make(map[uint64]int)
	go func() {
		for {
			m := reader.Pop()
			m.Done()
			uniqueMap[m.Id()] = uniqueMap[m.Id()] + 1
		}
	}()
	time.Sleep(15 * time.Second)

	for _, v := range uniqueMap {
		if v != 1 {
			panic("error reading")
		}
	}
}

func TestReaderService_loadDB(t *testing.T) {
	loaders := 4
	timeout := 5
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
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		LoaderCount: loaders,
	}
	ctxt, _ := context.WithTimeout(ctx, time.Second*time.Duration(timeout))
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	var lastFid uint64
	wg := sync.WaitGroup{}
	for i := 0; i < loaders*2; i++ {
		wg.Add(1)
		go func() {
			for {
				select {
				case <-ctxt.Done():
					wg.Done()
					return
				case m := <-r.blobListChan:
					lastFid = m.Fid
				}
			}
		}()
	}
	wg.Wait()
	exp := 2 * loaders * timeout
	assert.True(t, int(lastFid) >= exp)
}

func TestReaderService_sortChain(t *testing.T) {
	ctx := context.Background()
	loaders := 2
	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
			Compression: domain.CompressionOptions{
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize:  1000000,
		WaiterCount: 0,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)
	assert.NotNil(t, r)

	for i := 0; i < loaders; i++ {
		go func(i int) {
			for {
				_ = r.loadDB(i)
			}
		}(i)
	}

	go r.sorter(ctx)

	stuckTimer := time.NewTimer(2 * time.Second)
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

// Benchmarks

func BenchmarkReaderService_loaderDB_single(b *testing.B) {
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
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize: 1000000,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(b, err)
	r.blobListChan = make(chan *protobuf.BlobMessageList, 100000)

	count := uint64(0)
	timeout := 10
	ctxt, _ := context.WithTimeout(ctx, time.Second*time.Duration(timeout))
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

	go func() {
		for {
			if err := r.loadDB(0); err != nil {
				panic(err)
			}
			atomic.AddUint64(&count, 1)
		}
	}()

	<-ctxt.Done()
}

func TestReaderService_multithreading(t *testing.T) {
	utils.InitLogger("info")
	ctx := context.Background()
	loaders := 10
	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test",
			Logger: log.Logger,
			CTX:    ctx,
		},
		DB: domain.DataBaseOptions{
			DB: testDataBase,
			Compression: domain.CompressionOptions{
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize:  1000000,
		BatchSize:   50,
		LoaderCount: loaders,
		WaiterCount: loaders * 2,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

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
				log.Info().Int64("op-per-sec", int64(count-prev)).Int("buffer", len(r.bufferChan)).Int("blob list", len(r.blobListChan)).Int("out chan", len(r.outChan)).Msg("tick")
				prev = count
			}
		}
	}()

	for i := 0; i < loaders*2; i++ {
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

func BenchmarkReaderService_sortChain(b *testing.B) {

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
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize: 100000,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(b, err)
	r.blobListChan = make(chan *protobuf.BlobMessageList, 1000)

	var fids []uint64
	for i := 1; i < 20; i++ {
		fids = append(fids, uint64(i))
	}
	rand.Shuffle(len(fids), func(i, j int) {
		fids[i], fids[j] = fids[j], fids[i]
	})

	for _, fid := range fids {
		r.blobListChan <- &protobuf.BlobMessageList{
			Fid: fid,
		}
	}

	timelimit := 1
	timeout := time.NewTimer(time.Duration(timelimit) * time.Second)
	go r.sorter(ctx)
	<-timeout.C

}

func TestReaderService_getData(t *testing.T) {
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
				Compression:   domain.BLOB_COMPRESSION_GZIP,
				Encryption:    domain.BLOB_ENCRYPTION_AES,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		},
		BufferSize: 100000,
		BatchSize:  10,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	loaders := 1
	ctxt, _ := context.WithTimeout(ctx, time.Second*7)
	for i := 0; i < loaders; i++ {
		go func() {
			for {
				var batch []domain.Blob
				fid := atomic.AddUint64(&r.dbFid, r.batchSize)
				if err = r.getData(fid, &batch); err != nil {
					if err.Error() == "record not found" {
						return
					}
				}
				assert.Nil(t, err)
			}
		}()
	}
	<-ctxt.Done()
	log.Logger.Info().Int64("delta-time", r.deltaTime).Int64("delta-count", r.deltaTimeCount).Int64("ratio-ms", r.deltaTime/r.deltaTimeCount).Uint64("last fid", r.dbFid).Msg("test finished")
}

func TestReaderService_C(t *testing.T) {
	ctx := context.Background()
	l := utils.InitLogger("info")
	//l := zerolog.Nop()
	gLogger := utils.NewGLogger(l, true).LogMode(logger.Warn)
	connectionString := fmt.Sprintf("root:@tcp(localhost:3306)/axq?charset=utf8&parseTime=True&loc=Local")
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger})
	if err != nil {
		panic(err)
	}
	assert.Nil(t, err)

	w, err := NewWriterService(domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			CTX:    ctx,
			Logger: l,
			Name:   "global_test",
		},
		DB: domain.DataBaseOptions{
			DB: db,
			Compression: domain.CompressionOptions{
				Compression: domain.BLOB_COMPRESSION_GZIP,
			},
		},
		PartitionsCount: 4,
		MaxBlobSize:     10000,
	})
	go func() {
		for {
			time.Sleep(time.Millisecond * 1000)
			err = w.Push([]byte("test"))
			assert.Nil(t, err)
		}
	}()

	readerOpts := domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			CTX:    ctx,
			Logger: l,
			Name:   "global_test",
		},
		DB: domain.DataBaseOptions{
			DB: db,
			Compression: domain.CompressionOptions{
				Compression: domain.BLOB_COMPRESSION_GZIP,
			},
		},
		ReaderName:  "global_test_reader",
		LoaderCount: 1,
		WaiterCount: 1,
		BufferSize:  100_000,
		BatchSize:   10,
	}
	r, err := NewReaderService(readerOpts)
	assert.Nil(t, err)

	for {
		select {
		case msg := <-r.C():
			fmt.Println(msg.Id(), msg)
			msg.Done()
		}
	}

	//r := &mockReader{
	//	ctx:           ctx,
	//	db:            db,
	//	tableName:     "axq_global_test",
	//	batchSize:     10,
	//	nextBatchSize: 10,
	//}
	//
	//for {
	//	// 0 -> 10 (поиск от 1 до 10)
	//	fid := atomic.AddUint64(&r.dbFid, r.batchSize)
	//	var batch []domain.Blob
	//	for {
	//		err = r.testGetData(fid, &batch)
	//		assert.Nil(t, err)
	//		fmt.Println("from", fid-r.nextBatchSize, "to", fid, "found", len(batch), "next batch", r.nextBatchSize)
	//		if len(batch) != int(r.batchSize) {
	//			if len(batch) == 0 {
	//				time.Sleep(500 * time.Millisecond)
	//				continue
	//			}
	//			r.nextBatchSize -= uint64(len(batch))
	//		}
	//		fmt.Println("processing", len(batch))
	//		if len(batch) != int(r.batchSize) {
	//			if r.nextBatchSize == 0 {
	//				r.nextBatchSize = r.batchSize
	//				break
	//			}
	//			time.Sleep(100 * time.Millisecond)
	//			continue
	//		}
	//		break
	//	}
	//	time.Sleep(500 * time.Millisecond)
	//}
}

type mockReader struct {
	ctx           context.Context
	db            *gorm.DB
	dbFid         uint64
	tableName     string
	batchSize     uint64
	nextBatchSize uint64
}

func (r *mockReader) testGetData(fid uint64, res *[]domain.Blob) error {
	localCtx, cancelFn := context.WithTimeout(r.ctx, time.Second*5)
	defer cancelFn()
	return r.db.WithContext(localCtx).Table(r.tableName).Where("fid > ? AND fid <= ?", fid-r.nextBatchSize, fid).Find(res).Error
}
