package service

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axgrid/axq/domain"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// testQueue выдаёт уникальное имя очереди и убирает за собой всё, что сервисы
// после себя оставляют: таблицу блобов, строки счётчиков и запись владельца.
// Без изоляции почти все кейсы ниже невыразимы — они завязаны на то, пуста
// таблица или нет.
func testQueue(t testing.TB) string {
	t.Helper()
	id := uuid.New()
	name := fmt.Sprintf("t%x", id[:6])
	t.Cleanup(func() {
		testDataBase.Exec(fmt.Sprintf("DROP TABLE IF EXISTS axq_%s", name))
		testDataBase.Exec("DELETE FROM axq_counters WHERE name = ?", name)
		testDataBase.Exec("DELETE FROM allows WHERE writer_name = ?", name)
	})
	return name
}

// testCtx даёт контекст, который гарантированно отменяется по завершении теста,
// чтобы фоновые горутины сервисов не переживали свой тест.
func testCtx(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

func writerOpts(t testing.TB, name string) domain.WriterOptions {
	return domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   name,
			Logger: zerolog.Nop(),
			CTX:    testCtx(t),
		},
		DB:          domain.DataBaseOptions{DB: testDataBase},
		MaxBlobSize: 1000,
	}
}

func readerOpts(t testing.TB, name string) domain.ReaderOptions {
	return domain.ReaderOptions{
		BaseOptions: domain.BaseOptions{
			Name:   name,
			Logger: zerolog.Nop(),
			CTX:    testCtx(t),
		},
		DB:          domain.DataBaseOptions{DB: testDataBase},
		ReaderName:  name + "_r",
		BufferSize:  10_000,
		BatchSize:   100,
		LoaderCount: 2,
		WaiterCount: 1,
	}
}

func newWriter(t testing.TB, opts domain.WriterOptions) *WriterService {
	t.Helper()
	w, err := NewWriterService(opts)
	if err != nil {
		t.Fatalf("NewWriterService: %v", err)
	}
	return w
}

func newReader(t testing.TB, opts domain.ReaderOptions) *ReaderService {
	t.Helper()
	r, err := NewReaderService(opts)
	if err != nil {
		t.Fatalf("NewReaderService: %v", err)
	}
	return r
}

// pushN пишет n сообщений с предсказуемой полезной нагрузкой и возвращает её,
// проиндексированную по ожидаемому id (нумерация начинается с 1).
func pushN(t testing.TB, w *WriterService, n int) map[uint64][]byte {
	t.Helper()
	want := make(map[uint64][]byte, n)
	for i := 1; i <= n; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))
		if err := w.Push(msg); err != nil {
			t.Fatalf("Push #%d: %v", i, err)
		}
		want[uint64(i)] = msg
	}
	return want
}

// readIDs читает ровно n сообщений, подтверждая каждое, и возвращает id в том
// порядке, в котором они пришли.
func readIDs(t testing.TB, r *ReaderService, n int, timeout time.Duration) []uint64 {
	t.Helper()
	ids := make([]uint64, 0, n)
	deadline := time.After(timeout)
	for len(ids) < n {
		select {
		case m := <-r.C():
			ids = append(ids, m.Id())
			m.Done()
		case <-deadline:
			t.Fatalf("прочитано %d из %d сообщений за %s", len(ids), n, timeout)
		}
	}
	return ids
}

// expectNothing убеждается, что за отведённое время из ридера ничего не вышло.
func expectNothing(t testing.TB, r *ReaderService, within time.Duration) {
	t.Helper()
	select {
	case m := <-r.C():
		m.Done()
		t.Fatalf("ожидали тишину, а пришло сообщение id=%d", m.Id())
	case <-time.After(within):
	}
}

// assertContiguous проверяет главный инвариант очереди: множество id — это
// ровно непрерывный отрезок [from, from+len), без дыр и без повторов. Порядок
// поступления не проверяется, для него есть assertAscending.
func assertContiguous(t testing.TB, ids []uint64, from uint64) {
	t.Helper()
	seen := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("дубль id=%d", id)
		}
		seen[id] = true
	}
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for i, id := range sorted {
		if want := from + uint64(i); id != want {
			t.Fatalf("дыра в нумерации: на позиции %d ожидали id=%d, получили %d", i, want, id)
		}
	}
}

func assertAscending(t testing.TB, ids []uint64) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("порядок нарушен: ids[%d]=%d пришёл после ids[%d]=%d", i, ids[i], i-1, ids[i-1])
		}
	}
}

// blobsOf возвращает все блобы очереди по возрастанию fid.
func blobsOf(t testing.TB, name string) []domain.Blob {
	t.Helper()
	var blobs []domain.Blob
	if err := testDataBase.Table("axq_" + name).Order("fid asc").Find(&blobs).Error; err != nil {
		t.Fatalf("чтение блобов: %v", err)
	}
	return blobs
}

// pushConcurrent пишет n сообщений из workers горутин. Батчинг в райтере
// включается только при конкурентной записи: save() забирает из inChan столько,
// сколько там накопилось, поэтому последовательный Push даёт блоб на сообщение.
func pushConcurrent(t testing.TB, w *WriterService, n, workers int) {
	t.Helper()
	var (
		next int64
		mu   sync.Mutex
		bad  error
	)
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id := atomic.AddInt64(&next, 1)
				if id > int64(n) {
					return
				}
				if err := w.Push([]byte(fmt.Sprintf("msg-%d", id))); err != nil {
					mu.Lock()
					if bad == nil {
						bad = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if bad != nil {
		t.Fatalf("Push: %v", bad)
	}
}
