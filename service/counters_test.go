package service

import (
	"testing"
	"time"

	"github.com/axgrid/axq/domain"
	"github.com/rs/zerolog"
)

func newCounter(t *testing.T, name string, startFromEnd, everyTime, fromLatest bool) *CounterService {
	t.Helper()
	c, err := NewCounterService(name, name+"_c", testCtx(t), zerolog.Nop(), testDataBase, startFromEnd, everyTime, fromLatest)
	if err != nil {
		t.Fatalf("NewCounterService: %v", err)
	}
	return c
}

func waitCounter(t *testing.T, c *CounterService, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Last().Id == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("счётчик не дошёл до %d за %s, застрял на %d", want, timeout, c.Last().Id)
}

func assertCounterStays(t *testing.T, c *CounterService, want uint64, within time.Duration) {
	t.Helper()
	time.Sleep(within)
	if got := c.Last().Id; got != want {
		t.Fatalf("счётчик должен был стоять на %d, но уехал на %d", want, got)
	}
}

// C1: подтверждения приходят вразнобой (несколько outer-воркеров работают
// параллельно), но позиция обязана двигаться только по непрерывной цепочке.
func TestCounterService_SetOutOfOrder(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	for _, id := range []uint64{4, 2, 5, 1, 3} {
		c.Set(domain.MessageIDs{FID: 1, Id: id})
	}
	waitCounter(t, c, 5, 2*time.Second)
}

// C2: пока в цепочке дыра, позиция стоит — иначе рестарт потерял бы
// неподтверждённое сообщение.
func TestCounterService_GapHoldsPosition(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	// id=3 не подтверждён
	for _, id := range []uint64{1, 2, 4, 5, 6} {
		c.Set(domain.MessageIDs{FID: 1, Id: id})
	}
	waitCounter(t, c, 2, 2*time.Second)
	assertCounterStays(t, c, 2, 300*time.Millisecond)
}

// C2b: когда дыра закрывается, счётчик обязан догнать всю цепочку разом.
// Именно здесь старая реализация уходила в спин и самоблокировку: id с
// разрывом возвращались в тот же канал, из которого читались.
func TestCounterService_GapFillsAndCatchesUp(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	for _, id := range []uint64{1, 2, 4, 5, 6} {
		c.Set(domain.MessageIDs{FID: 1, Id: id})
	}
	waitCounter(t, c, 2, 2*time.Second)

	c.Set(domain.MessageIDs{FID: 1, Id: 3})
	waitCounter(t, c, 6, 2*time.Second)
}

// C2c: разрыв больше ёмкости канала подтверждений. Старая реализация тут
// вставала намертво: канал забивался возвращёнными id и set() блокировался
// на записи в него же.
func TestCounterService_LargeGapDoesNotDeadlock(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	const total = 20_000 // вдвое больше ёмкости lastIdChan
	// сначала всё, кроме первого — цепочка не может сдвинуться ни на шаг
	for id := uint64(2); id <= total; id++ {
		c.Set(domain.MessageIDs{FID: 1, Id: id})
	}
	assertCounterStays(t, c, 0, 200*time.Millisecond)

	c.Set(domain.MessageIDs{FID: 1, Id: 1})
	waitCounter(t, c, total, 10*time.Second)
}

// C3: Commit двигает позицию сразу на конец непрерывного диапазона — так
// архивер отмечает целый залитый блоб одним шагом вместо цикла по каждому id.
func TestCounterService_CommitJumpsForward(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	c.Commit(domain.MessageIDs{FID: 7, Id: 100_000})
	waitCounter(t, c, 100_000, 2*time.Second)

	if fid := c.Last().FID; fid != 7 {
		t.Fatalf("Commit не перенёс FID: got %d, want 7", fid)
	}
}

// C3b: Commit назад игнорируется, иначе повторная доставка отбросила бы
// позицию и сообщения поехали бы по второму разу.
func TestCounterService_CommitNeverGoesBack(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	c.Commit(domain.MessageIDs{FID: 2, Id: 500})
	waitCounter(t, c, 500, 2*time.Second)

	c.Commit(domain.MessageIDs{FID: 1, Id: 100})
	assertCounterStays(t, c, 500, 300*time.Millisecond)
}

// C3c: Commit и Set работают по одной позиции — то, что уже накоммичено,
// не должно откатываться отставшими подтверждениями.
func TestCounterService_CommitThenStaleSet(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	c.Commit(domain.MessageIDs{FID: 3, Id: 1000})
	waitCounter(t, c, 1000, 2*time.Second)

	for _, id := range []uint64{10, 500, 999} {
		c.Set(domain.MessageIDs{FID: 3, Id: id})
	}
	assertCounterStays(t, c, 1000, 300*time.Millisecond)

	c.Set(domain.MessageIDs{FID: 3, Id: 1001})
	waitCounter(t, c, 1001, 2*time.Second)
}

// Позиция должна переживать перезапуск: save() сбрасывает её в базу, и новый
// счётчик с тем же именем обязан её подобрать.
func TestCounterService_PersistsAcrossRestart(t *testing.T) {
	name := testQueue(t)
	c := newCounter(t, name, false, false, false)

	c.Commit(domain.MessageIDs{FID: 4, Id: 777})
	waitCounter(t, c, 777, 2*time.Second)

	// save() ходит в базу раз в 3 секунды
	time.Sleep(4 * time.Second)

	restored := newCounter(t, name, false, false, false)
	if got := restored.Last().Id; got != 777 {
		t.Fatalf("после перезапуска позиция %d, ожидали 777", got)
	}
}

// Пустая очередь: StartFromEnd не за что зацепиться, счётчик обязан встать
// на ноль, а не развалиться.
func TestCounterService_StartFromEnd_EmptyQueue(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name)) // создаёт пустую таблицу
	_ = w

	c := newCounter(t, name, true, false, false)
	if got := c.Last().Id; got != 0 {
		t.Fatalf("на пустой очереди позиция %d, ожидали 0", got)
	}
}

// StartFromEnd на непустой очереди встаёт на последнее записанное сообщение.
func TestCounterService_StartFromEnd_WithData(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 50)

	c := newCounter(t, name, true, false, false)
	if got := c.Last().Id; got != 50 {
		t.Fatalf("StartFromEnd дал позицию %d, ожидали 50", got)
	}
}

// StartFromEndEveryTime перебивает уже сохранённую позицию на каждом старте.
func TestCounterService_StartFromEndEveryTime(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 30)

	first := newCounter(t, name, true, false, false)
	if got := first.Last().Id; got != 30 {
		t.Fatalf("первый старт дал позицию %d, ожидали 30", got)
	}
	pushN(t, w, 20) // теперь в очереди 50

	// без флага позиция берётся из базы, с флагом — с конца очереди
	again := newCounter(t, name, true, true, false)
	if got := again.Last().Id; got != 50 {
		t.Fatalf("StartFromEndEveryTime дал позицию %d, ожидали 50", got)
	}
}

// FromLatest — тот же смысл, что StartFromEnd+EveryTime, но без записи в базу.
func TestCounterService_FromLatest_WithData(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 42)

	c := newCounter(t, name, false, false, true)
	if got := c.Last().Id; got != 42 {
		t.Fatalf("FromLatest дал позицию %d, ожидали 42", got)
	}
}

// КРАСНЫЙ: FromLatest на пустой очереди роняет конструктор.
// counters.go:66-70 возвращает наружу gorm.ErrRecordNotFound, хотя соседняя
// ветка StartFromEnd этот же случай глотает и стартует с нуля. Ридер с
// FromLatest, поднятый раньше райтера, просто не создаётся.
func TestCounterService_FromLatest_EmptyQueue(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name)) // создаёт пустую таблицу
	_ = w

	c, err := NewCounterService(name, name+"_c", testCtx(t), zerolog.Nop(), testDataBase, false, false, true)
	if err != nil {
		t.Fatalf("FromLatest на пустой очереди вернул ошибку %v, ожидали старт с нуля", err)
	}
	if got := c.Last().Id; got != 0 {
		t.Fatalf("на пустой очереди позиция %d, ожидали 0", got)
	}
}
