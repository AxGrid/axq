package service

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axgrid/axq/domain"
)

// R1: базовый инвариант — прочитано ровно то, что записано, по возрастанию,
// без дублей и пропусков.
func TestReader_ReadsEverythingInOrder(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 1000
	pushN(t, w, n)

	r := newReader(t, readerOpts(t, name))
	ids := readIDs(t, r, n, 30*time.Second)

	assertContiguous(t, ids, 1)
	assertAscending(t, ids)
}

// R2: ридер, поднятый раньше райтера, обязан дождаться хвоста, а не сдаться.
func TestReader_WaitsForTail(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name)) // создаёт пустую таблицу
	r := newReader(t, readerOpts(t, name))

	expectNothing(t, r, 300*time.Millisecond)

	const n = 100
	pushN(t, w, n)

	ids := readIDs(t, r, n, 30*time.Second)
	assertContiguous(t, ids, 1)
}

// R15: на пустой очереди ридер просто ждёт и ничего не выдумывает.
func TestReader_EmptyQueueStaysQuiet(t *testing.T) {
	name := testQueue(t)
	newWriter(t, writerOpts(t, name))

	r := newReader(t, readerOpts(t, name))
	expectNothing(t, r, time.Second)
}

// R3: перезапуск с тем же именем продолжает с сохранённой позиции, а не
// перечитывает очередь заново.
func TestReader_RestartContinuesFromCounter(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 200)

	ctx, cancel := context.WithCancel(context.Background())
	opts := readerOpts(t, name)
	opts.CTX = ctx
	first := newReader(t, opts)
	readIDs(t, first, 100, 30*time.Second)

	// счётчик сбрасывается в базу раз в 3 секунды
	time.Sleep(4 * time.Second)
	cancel()
	time.Sleep(200 * time.Millisecond)

	second := newReader(t, readerOpts(t, name))
	ids := readIDs(t, second, 100, 30*time.Second)

	if ids[0] <= 100 {
		t.Fatalf("после перезапуска пришёл id=%d — очередь перечитывается с начала", ids[0])
	}
	assertAscending(t, ids)
}

// R4: сообщения, не подтверждённые до падения, обязаны прийти снова —
// это at-least-once, ради которого счётчик и двигается только по ack.
func TestReader_UnackedAreRedelivered(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 50)

	ctx, cancel := context.WithCancel(context.Background())
	opts := readerOpts(t, name)
	opts.CTX = ctx

	// выдающий воркер отдаёт следующее сообщение только после ack предыдущего,
	// поэтому неподтверждённым можно удержать ровно одно
	first := newReader(t, opts)
	var unacked uint64
	select {
	case m := <-first.C():
		unacked = m.Id()
		// намеренно без Done — имитируем падение потребителя
	case <-time.After(30 * time.Second):
		t.Fatal("не дождались сообщения")
	}
	cancel()
	time.Sleep(4 * time.Second) // дать счётчику шанс сохраниться

	second := newReader(t, readerOpts(t, name))
	select {
	case m := <-second.C():
		defer m.Done()
		if m.Id() > unacked {
			t.Fatalf("после перезапуска пришёл id=%d, а неподтверждённым остался %d — сообщение потеряно", m.Id(), unacked)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("после перезапуска не пришло ничего")
	}
}

// R9: Error возвращает сообщение в оборот, счётчик при этом стоять.
func TestReader_ErrorRedelivers(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 10)

	r := newReader(t, readerOpts(t, name))

	var first uint64
	select {
	case m := <-r.C():
		first = m.Id()
		m.Error(fmt.Errorf("обработчик не смог"))
	case <-time.After(30 * time.Second):
		t.Fatal("не дождались первого сообщения")
	}

	select {
	case again := <-r.C():
		defer again.Done()
		if again.Id() != first {
			t.Fatalf("после Error пришёл id=%d, ожидали повтор %d", again.Id(), first)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("после Error сообщение не вернулось")
	}
}

// R5: StartFromEnd пропускает всё, что было записано до старта.
func TestReader_StartFromEnd(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 100)

	opts := readerOpts(t, name)
	opts.StartFromEnd = true
	r := newReader(t, opts)

	expectNothing(t, r, 500*time.Millisecond)

	if err := w.Push([]byte("свежее")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	select {
	case m := <-r.C():
		defer m.Done()
		if m.Id() != 101 {
			t.Fatalf("пришёл id=%d, ожидали 101", m.Id())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("свежее сообщение не пришло")
	}
}

// R8: явная позиция задаёт точку старта в обход счётчика.
func TestReader_ExplicitLastId(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.MaxBlobSize = 10
	w := newWriter(t, opts)
	pushN(t, w, 100)

	ropts := readerOpts(t, name)
	ropts.LastId = &domain.LastIdOptions{FID: 6, LastId: 50}
	r := newReader(t, ropts)

	ids := readIDs(t, r, 50, 30*time.Second)
	assertContiguous(t, ids, 51)
}

// R18: если стартовая позиция ниже реально прочитанного, блобы перечитываются.
// Уже отданные id обязаны отсеиваться, а не копиться в waitMap — именно так
// ридер съедал память на каждом рестарте архивера.
func TestReader_RereadSkipsAlreadyDeliveredIDs(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.MaxBlobSize = 10
	w := newWriter(t, opts)
	pushN(t, w, 100)

	// FID=1 заставляет загрузчики перечитать таблицу с самого начала,
	// хотя позиция по сообщениям стоит на 50
	ropts := readerOpts(t, name)
	ropts.LastId = &domain.LastIdOptions{FID: 1, LastId: 50}
	r := newReader(t, ropts)

	ids := readIDs(t, r, 50, 30*time.Second)
	for _, id := range ids {
		if id <= 50 {
			t.Fatalf("пришёл уже обработанный id=%d", id)
		}
	}
	assertContiguous(t, ids, 51)
}

// R10: разные потребители читают один поток независимо друг от друга.
func TestReader_IndependentReaderNames(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 100
	pushN(t, w, n)

	optsA := readerOpts(t, name)
	optsA.ReaderName = name + "_a"
	optsB := readerOpts(t, name)
	optsB.ReaderName = name + "_b"

	a := newReader(t, optsA)
	b := newReader(t, optsB)

	idsA := readIDs(t, a, n, 30*time.Second)
	idsB := readIDs(t, b, n, 30*time.Second)

	assertContiguous(t, idsA, 1)
	assertContiguous(t, idsB, 1)
}

// R11: параллельные загрузчики не ломают порядок — блобы приезжают вразнобой,
// но наружу sorter обязан выдавать строго по возрастанию.
func TestReader_ManyLoadersKeepOrder(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 10
	w := newWriter(t, wopts)
	const n = 500
	pushN(t, w, n)

	opts := readerOpts(t, name)
	opts.LoaderCount = 8
	r := newReader(t, opts)

	ids := readIDs(t, r, n, 60*time.Second)
	assertContiguous(t, ids, 1)
	assertAscending(t, ids)
}

// R12: при нескольких выдающих воркерах порядок между ними не гарантируется —
// но ни одно сообщение не должно потеряться или прийти дважды.
func TestReader_ManyWaitersNoLossNoDuplicates(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 10
	w := newWriter(t, wopts)
	const n = 500
	pushN(t, w, n)

	opts := readerOpts(t, name)
	opts.WaiterCount = 8
	r := newReader(t, opts)

	var (
		mu  sync.Mutex
		ids []uint64
	)
	// отмена по контексту видна всем сборщикам сразу, в отличие от таймера,
	// который достался бы только одному
	collectCtx, collectCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer collectCancel()

	wg := sync.WaitGroup{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case m := <-r.C():
					mu.Lock()
					ids = append(ids, m.Id())
					enough := len(ids) >= n
					mu.Unlock()
					m.Done()
					if enough {
						collectCancel()
						return
					}
				case <-collectCtx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(ids) < n {
		t.Fatalf("прочитано %d из %d", len(ids), n)
	}
	assertContiguous(t, ids[:n], 1)
}

// R16 — КРАСНЫЙ: на нечитаемом блобе загрузчики срываются в плотный цикл.
//
// Ошибку расшифровки loadDB обрабатывает голым `continue` внутреннего цикла:
// ни паузы, ни проверки ctx. Тот же fid запрашивается снова и снова на полной
// скорости, и так до конца жизни процесса — отмена контекста это не
// останавливает. Один такой ридер загружает базу на весь оставшийся прогон.
func TestReader_WrongEncryptionKeyDoesNotCrash(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.DB.Compression = domain.CompressionOptions{
		Encryption:    domain.BLOB_ENCRYPTION_AES,
		EncryptionKey: []byte("11111111111111111111111111111111"),
	}
	w := newWriter(t, wopts)
	pushN(t, w, 10)

	ropts := readerOpts(t, name)
	ropts.DB.Compression = domain.CompressionOptions{
		Encryption:    domain.BLOB_ENCRYPTION_AES,
		EncryptionKey: []byte("22222222222222222222222222222222"),
	}
	r := newReader(t, ropts)

	// блоб не разбирается, наружу выйти нечему
	expectNothing(t, r, time.Second)

	before := atomic.LoadInt64(&r.deltaTimeCount)
	time.Sleep(time.Second)
	queries := atomic.LoadInt64(&r.deltaTimeCount) - before

	// с паузой в 100мс двум загрузчикам хватило бы порядка двух десятков
	// запросов в секунду
	if queries > 200 {
		t.Fatalf("за секунду ридер сходил в базу %d раз — загрузчики повторяют попытку без паузы", queries)
	}
}

// R14: если начало очереди удалили, ридер обязан перескочить на первый живой
// блоб, а не ждать вырезанный fid вечно.
func TestReader_SkipsDeletedHead(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 10
	w := newWriter(t, wopts)
	pushN(t, w, 200)

	blobs := blobsOf(t, name)
	if len(blobs) < 5 {
		t.Fatalf("ожидали хотя бы 5 блобов, получили %d", len(blobs))
	}
	cutTo := blobs[2].FID
	if err := testDataBase.Table("axq_"+name).Where("fid <= ?", cutTo).Delete(&domain.Blob{}).Error; err != nil {
		t.Fatalf("удаление головы: %v", err)
	}
	firstAlive := blobs[3]

	r := newReader(t, readerOpts(t, name))
	select {
	case m := <-r.C():
		defer m.Done()
		if m.Id() != firstAlive.FromId {
			t.Fatalf("после вырезанной головы пришёл id=%d, ожидали %d", m.Id(), firstAlive.FromId)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("ридер завис на вырезанном начале очереди")
	}
}

// R13: дыра в середине таблицы. Ридер обязан отдать всё до неё и не свалиться;
// дальше он останавливается намеренно — пропустить дыру значило бы молча
// потерять сообщения, о которых мы ничего не знаем.
func TestReader_StopsAtHoleInMiddle(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 10
	w := newWriter(t, wopts)
	pushN(t, w, 200)

	blobs := blobsOf(t, name)
	hole := blobs[5]
	if err := testDataBase.Table("axq_"+name).Where("fid = ?", hole.FID).Delete(&domain.Blob{}).Error; err != nil {
		t.Fatalf("удаление блоба: %v", err)
	}

	r := newReader(t, readerOpts(t, name))
	before := int(hole.FromId - 1)
	ids := readIDs(t, r, before, 30*time.Second)
	assertContiguous(t, ids, 1)

	// за дырой поток останавливается
	expectNothing(t, r, 2*time.Second)
}

// R17 — КРАСНЫЙ: отмена контекста не останавливает загрузчики.
//
// loadDB, не найдя блоб, спит и повторяет попытку в цикле, который вообще не
// смотрит на ctx. После отмены getData начинает мгновенно возвращать
// context.Canceled, цикл срывается в ветку `time.Sleep(250ms); continue` —
// и горутина остаётся крутиться навсегда.
func TestReader_CancelStopsGoroutines(t *testing.T) {
	name := testQueue(t)
	newWriter(t, writerOpts(t, name))

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	opts := readerOpts(t, name)
	opts.CTX = ctx
	opts.LoaderCount = 4
	newReader(t, opts)

	time.Sleep(time.Second) // дать загрузчикам встать на ожидание хвоста
	cancel()
	time.Sleep(2 * time.Second)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("после отмены осталось %d горутин против %d до старта ридера", after, before)
	}
}
