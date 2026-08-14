package service

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Нагрузочные тесты идут минутами и держат базу занятой, поэтому в обычном
// прогоне они пропускаются. Включение: AX_STRESS=1 go test ./service/ -run Stress -v
const stressEnv = "AX_STRESS"

func requireStress(t *testing.T) {
	t.Helper()
	if os.Getenv(stressEnv) == "" {
		t.Skipf("нагрузочный тест; включается через %s=1", stressEnv)
	}
}

// stressDuration — сколько лить нагрузку. AX_STRESS_SECONDS переопределяет.
func stressDuration() time.Duration {
	if v := os.Getenv("AX_STRESS_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 15 * time.Second
}

func rate(count int64, elapsed time.Duration) float64 {
	return float64(count) / elapsed.Seconds()
}

// Запись под нагрузкой: сколько сообщений в секунду принимает райтер при разном
// числе писателей, и цела ли после этого нумерация.
func TestStress_WriteThroughput(t *testing.T) {
	requireStress(t)
	dur := stressDuration()

	for _, writers := range []int{1, 8, 64, 256, 1024} {
		t.Run(fmt.Sprintf("writers=%d", writers), func(t *testing.T) {
			name := testQueue(t)
			w := newWriter(t, writerOpts(t, name))

			var acked int64
			stop := make(chan struct{})
			wg := sync.WaitGroup{}
			start := time.Now()
			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						if err := w.Push([]byte(fmt.Sprintf("stress-%d-%d", i, n))); err != nil {
							return
						}
						atomic.AddInt64(&acked, 1)
					}
				}(i)
			}
			time.Sleep(dur)
			close(stop)
			wg.Wait()
			elapsed := time.Since(start)

			total := atomic.LoadInt64(&acked)
			t.Logf("писателей %4d: %8d сообщений за %v = %.0f msg/s", writers, total, elapsed.Round(time.Millisecond), rate(total, elapsed))

			// нагрузка не должна нарушать главный инвариант
			lastId, err := w.LastID()
			if err != nil {
				t.Fatalf("LastID: %v", err)
			}
			if lastId != uint64(total) {
				t.Fatalf("подтверждено %d сообщений, а в очереди %d", total, lastId)
			}
			assertBlobChainContiguous(t, name, int(total))
		})
	}
}

// Чтение под нагрузкой: очередь уже заполнена, меряем чистую скорость вычитки.
func TestStress_ReadThroughput(t *testing.T) {
	requireStress(t)

	const total = 200_000
	for _, tc := range []struct{ loaders, waiters int }{
		{1, 1}, {4, 1}, {8, 4}, {16, 16},
	} {
		t.Run(fmt.Sprintf("loaders=%d/waiters=%d", tc.loaders, tc.waiters), func(t *testing.T) {
			name := testQueue(t)
			wopts := writerOpts(t, name)
			wopts.MaxBlobSize = 1000
			w := newWriter(t, wopts)

			fillStart := time.Now()
			pushConcurrent(t, w, total, 256)
			t.Logf("наполнение: %d сообщений за %v", total, time.Since(fillStart).Round(time.Millisecond))

			ropts := readerOpts(t, name)
			ropts.LoaderCount = tc.loaders
			ropts.WaiterCount = tc.waiters
			r := newReader(t, ropts)

			var (
				mu   sync.Mutex
				seen = make(map[uint64]bool, total)
			)
			done := make(chan struct{})
			var closeOnce sync.Once
			wg := sync.WaitGroup{}
			start := time.Now()
			for i := 0; i < tc.waiters; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-done:
							return
						case m := <-r.C():
							mu.Lock()
							dup := seen[m.Id()]
							seen[m.Id()] = true
							full := len(seen) == total
							mu.Unlock()
							m.Done()
							if dup {
								t.Errorf("дубль id=%d", m.Id())
								closeOnce.Do(func() { close(done) })
								return
							}
							if full {
								closeOnce.Do(func() { close(done) })
								return
							}
						case <-time.After(2 * time.Minute):
							t.Error("чтение не уложилось в 2 минуты")
							closeOnce.Do(func() { close(done) })
							return
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			mu.Lock()
			got := len(seen)
			mu.Unlock()
			t.Logf("загрузчиков %2d, выдающих %2d: %8d сообщений за %v = %.0f msg/s",
				tc.loaders, tc.waiters, got, elapsed.Round(time.Millisecond), rate(int64(got), elapsed))

			if got != total {
				t.Fatalf("прочитано %d из %d", got, total)
			}
		})
	}
}

// Запись и чтение одновременно — режим, в котором очередь живёт на самом деле.
// Проверяем не только скорость, но и что ридер не обгоняет райтера и не теряет.
func TestStress_WriteReadPipeline(t *testing.T) {
	requireStress(t)
	dur := stressDuration()

	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 1000
	w := newWriter(t, wopts)

	ropts := readerOpts(t, name)
	ropts.LoaderCount = 8
	ropts.WaiterCount = 8
	r := newReader(t, ropts)

	var (
		written int64
		mu      sync.Mutex
		seen    = make(map[uint64]bool)
		maxId   uint64
	)

	stop := make(chan struct{})
	writersWG := sync.WaitGroup{}
	readersWG := sync.WaitGroup{}
	start := time.Now()

	for i := 0; i < 128; i++ {
		writersWG.Add(1)
		go func(i int) {
			defer writersWG.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := w.Push([]byte(fmt.Sprintf("pipe-%d-%d", i, n))); err != nil {
					return
				}
				atomic.AddInt64(&written, 1)
			}
		}(i)
	}

	readStop := make(chan struct{})
	for i := 0; i < ropts.WaiterCount; i++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-readStop:
					return
				case m := <-r.C():
					mu.Lock()
					if seen[m.Id()] {
						t.Errorf("дубль id=%d", m.Id())
					}
					seen[m.Id()] = true
					if m.Id() > maxId {
						maxId = m.Id()
					}
					mu.Unlock()
					m.Done()
				}
			}
		}()
	}

	time.Sleep(dur)
	close(stop)
	writersWG.Wait()
	writeElapsed := time.Since(start)
	total := atomic.LoadInt64(&written)

	// дать ридеру догнать хвост
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		mu.Lock()
		caught := len(seen) >= int(total)
		mu.Unlock()
		if caught {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(readStop)
	readersWG.Wait()
	readElapsed := time.Since(start)

	mu.Lock()
	got, top := len(seen), maxId
	mu.Unlock()

	t.Logf("запись:  %8d сообщений за %v = %.0f msg/s", total, writeElapsed.Round(time.Millisecond), rate(total, writeElapsed))
	t.Logf("чтение:  %8d сообщений за %v = %.0f msg/s", got, readElapsed.Round(time.Millisecond), rate(int64(got), readElapsed))

	if top > uint64(total) {
		t.Fatalf("ридер выдал id=%d, а записано только %d — прочитано несуществующее", top, total)
	}
	if got != int(total) {
		t.Fatalf("записано %d, прочитано %d — ридер не догнал очередь", total, got)
	}
	// множество прочитанного обязано быть сплошным отрезком с единицы
	ids := make([]uint64, 0, got)
	mu.Lock()
	for id := range seen {
		ids = append(ids, id)
	}
	mu.Unlock()
	assertContiguous(t, ids, 1)
}
