package utils

import (
	"sync"
	"testing"
	"time"
)

// drain собирает всё, что MinimalId отдаёт наружу, пока не наступит тишина.
func drain(t *testing.T, m *MinimalId[uint64], quiet time.Duration) []uint64 {
	t.Helper()
	var (
		mu  sync.Mutex
		out []uint64
	)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case v := <-m.C():
				mu.Lock()
				out = append(out, v)
				mu.Unlock()
			}
		}
	}()
	time.Sleep(quiet)
	close(done)
	mu.Lock()
	defer mu.Unlock()
	return append([]uint64(nil), out...)
}

// C5: позиция двигается только по непрерывной цепочке, в каком бы порядке id
// ни приходили.
func TestMinimalId_AddOutOfOrder(t *testing.T) {
	m := NewMinimalId[uint64](0)
	go func() {
		for _, id := range []uint64{3, 5, 1, 2, 4} {
			m.Add(id)
		}
	}()
	got := drain(t, m, 200*time.Millisecond)

	want := []uint64{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("отдано %v, ожидали %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("отдано %v, ожидали %v", got, want)
		}
	}
	if m.Current() != 5 {
		t.Fatalf("Current()=%d, ожидали 5", m.Current())
	}
}

// C5: пока в цепочке дыра, наружу не уходит ничего, что лежит за ней.
func TestMinimalId_GapHoldsChain(t *testing.T) {
	m := NewMinimalId[uint64](0)
	go func() {
		// id=1 не приходит, значит и 2, 3 выйти не могут
		for _, id := range []uint64{2, 3} {
			m.Add(id)
		}
	}()
	if got := drain(t, m, 200*time.Millisecond); len(got) != 0 {
		t.Fatalf("при дыре в цепочке наружу ушло %v, ожидали пусто", got)
	}
	if m.Current() != 0 {
		t.Fatalf("Current()=%d, ожидали 0", m.Current())
	}
}

// C5: Forward перескакивает вырезанный участок и отпускает цепочку дальше.
func TestMinimalId_Forward(t *testing.T) {
	m := NewMinimalId[uint64](0)
	go func() {
		m.Add(101)
		// 1..100 удалены из очереди и не придут никогда
		m.Forward(100)
		m.Add(102)
	}()
	got := drain(t, m, 200*time.Millisecond)

	if len(got) != 2 || got[0] != 101 || got[1] != 102 {
		t.Fatalf("после Forward отдано %v, ожидали [101 102]", got)
	}
	if m.Current() != 102 {
		t.Fatalf("Current()=%d, ожидали 102", m.Current())
	}
}

// C5: Forward двигает только вперёд — назад позицию не сдвинуть, иначе
// сообщения поехали бы по второму разу.
func TestMinimalId_ForwardNeverGoesBack(t *testing.T) {
	m := NewMinimalId[uint64](500)
	m.Forward(100)
	if m.Current() != 500 {
		t.Fatalf("Forward(100) сдвинул позицию с 500 на %d", m.Current())
	}
	m.Forward(500)
	if m.Current() != 500 {
		t.Fatalf("Forward(500) в текущую позицию изменил её на %d", m.Current())
	}
}

// C5: Forward чистит за собой всё, что осталось ниже новой позиции, иначе
// отброшенные id копились бы в sortedMap до конца жизни процесса.
func TestMinimalId_ForwardDropsStaleEntries(t *testing.T) {
	m := NewMinimalId[uint64](0)
	go func() {
		for _, id := range []uint64{10, 20, 30} {
			m.Add(id)
		}
		m.Forward(25)
	}()
	drain(t, m, 200*time.Millisecond)

	m.sortedMapMu.Lock()
	defer m.sortedMapMu.Unlock()
	for id := range m.sortedMap {
		if id <= 25 {
			t.Fatalf("в sortedMap остался отброшенный id=%d", id)
		}
	}
}
