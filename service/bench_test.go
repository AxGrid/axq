package service

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axgrid/axq/domain"
)

// benchPayload — типичное небольшое сообщение. Размер вынесен в переменную,
// чтобы разница между прогонами объяснялась кодом, а не полезной нагрузкой.
var benchPayload = []byte("the quick brown fox jumps over the lazy dog")

// reportRate переводит замер в сообщения в секунду — метрику, в которой очередь
// обсуждают, в отличие от ns/op.
func reportRate(b *testing.B, messages int, elapsed time.Duration) {
	b.ReportMetric(float64(messages)/elapsed.Seconds(), "msg/s")
}

// BenchmarkWriter_Push — нижняя граница: последовательная запись без батчинга.
// Каждое сообщение уезжает своим блобом, то есть это ровно стоимость одного
// round-trip до базы.
func BenchmarkWriter_Push(b *testing.B) {
	name := testQueue(b)
	w := newWriter(b, writerOpts(b, name))

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if err := w.Push(benchPayload); err != nil {
			b.Fatalf("Push: %v", err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportRate(b, b.N, elapsed)
}

// benchPush гоняет b.N записей ровно через writers горутин. RunParallel тут не
// годится: SetParallelism(p) поднимает p*GOMAXPROCS горутин, и подписи вроде
// «writers=1» означали бы совсем не то, что написано.
func benchPush(b *testing.B, w *WriterService, writers int) {
	b.Helper()
	var next int64
	wg := sync.WaitGroup{}

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.AddInt64(&next, 1) <= int64(b.N) {
				if err := w.Push(benchPayload); err != nil {
					b.Errorf("Push: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	reportRate(b, b.N, elapsed)
}

// BenchmarkWriter_PushParallel — рабочий режим: чем больше писателей, тем
// крупнее батч, который save() успевает набрать из inChan за один поход в базу.
func BenchmarkWriter_PushParallel(b *testing.B) {
	for _, writers := range []int{1, 8, 32, 128, 512, 2048} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			name := testQueue(b)
			w := newWriter(b, writerOpts(b, name))
			benchPush(b, w, writers)
		})
	}
}

// BenchmarkWriter_PushMany — батчевый API: одна отправка вместо N ожиданий.
func BenchmarkWriter_PushMany(b *testing.B) {
	for _, batch := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			name := testQueue(b)
			w := newWriter(b, writerOpts(b, name))

			msgs := make([][]byte, batch)
			for i := range msgs {
				msgs[i] = benchPayload
			}
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				if err := w.PushMany(msgs); err != nil {
					b.Fatalf("PushMany: %v", err)
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()
			reportRate(b, b.N*batch, elapsed)
		})
	}
}

// BenchmarkWriter_Compression — во что обходятся gzip и AES на пути записи.
func BenchmarkWriter_Compression(b *testing.B) {
	key := []byte("12345678901234567890123456789012")
	for _, tc := range []struct {
		title       string
		compression domain.BlobCompression
		encryption  domain.BlobEncryption
	}{
		{"none", domain.BLOB_COMPRESSION_NONE, domain.BLOB_ENCRYPTION_NONE},
		{"gzip", domain.BLOB_COMPRESSION_GZIP, domain.BLOB_ENCRYPTION_NONE},
		{"aes", domain.BLOB_COMPRESSION_NONE, domain.BLOB_ENCRYPTION_AES},
		{"gzip+aes", domain.BLOB_COMPRESSION_GZIP, domain.BLOB_ENCRYPTION_AES},
	} {
		b.Run(tc.title, func(b *testing.B) {
			name := testQueue(b)
			opts := writerOpts(b, name)
			opts.DB.Compression = domain.CompressionOptions{
				Compression:   tc.compression,
				Encryption:    tc.encryption,
				EncryptionKey: key,
			}
			benchPush(b, newWriter(b, opts), 128)
		})
	}
}

// BenchmarkReader_Read — чтение уже записанной очереди. Наполнение идёт вне
// замера, иначе мерялась бы скорость райтера.
func BenchmarkReader_Read(b *testing.B) {
	name := testQueue(b)
	wopts := writerOpts(b, name)
	w := newWriter(b, wopts)
	pushConcurrent(b, w, b.N, 256)

	b.ResetTimer()
	start := time.Now()
	r := newReader(b, readerOpts(b, name))
	for i := 0; i < b.N; i++ {
		m := <-r.C()
		m.Done()
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportRate(b, b.N, elapsed)
}

// BenchmarkReader_Loaders — сколько дают параллельные загрузчики. Блобы мелкие,
// поэтому узкое место здесь именно чтение из базы, а не разбор.
func BenchmarkReader_Loaders(b *testing.B) {
	for _, loaders := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("loaders=%d", loaders), func(b *testing.B) {
			name := testQueue(b)
			wopts := writerOpts(b, name)
			wopts.MaxBlobSize = 100
			w := newWriter(b, wopts)
			pushConcurrent(b, w, b.N, 256)

			ropts := readerOpts(b, name)
			ropts.LoaderCount = loaders

			b.ResetTimer()
			start := time.Now()
			r := newReader(b, ropts)
			for i := 0; i < b.N; i++ {
				m := <-r.C()
				m.Done()
			}
			elapsed := time.Since(start)
			b.StopTimer()
			reportRate(b, b.N, elapsed)
		})
	}
}

// BenchmarkReader_Waiters — выдающие воркеры отдают сообщение и ждут ack,
// поэтому их число ограничивает число сообщений в полёте.
func BenchmarkReader_Waiters(b *testing.B) {
	for _, waiters := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			name := testQueue(b)
			wopts := writerOpts(b, name)
			wopts.MaxBlobSize = 100
			w := newWriter(b, wopts)
			pushConcurrent(b, w, b.N, 256)

			ropts := readerOpts(b, name)
			ropts.LoaderCount = 8
			ropts.WaiterCount = waiters

			var read int64
			b.ResetTimer()
			start := time.Now()
			r := newReader(b, ropts)
			done := make(chan struct{})
			for i := 0; i < waiters; i++ {
				go func() {
					for {
						select {
						case <-done:
							return
						case m := <-r.C():
							m.Done()
							if atomic.AddInt64(&read, 1) == int64(b.N) {
								close(done)
								return
							}
						}
					}
				}()
			}
			<-done
			elapsed := time.Since(start)
			b.StopTimer()
			reportRate(b, b.N, elapsed)
		})
	}
}
