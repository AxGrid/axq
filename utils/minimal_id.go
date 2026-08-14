package utils

import (
	"golang.org/x/exp/constraints"
	"sync"
)

type MinimalId[T constraints.Integer] struct {
	// current пишут только Add и Forward, оба под sortedMapMu, поэтому внутри
	// них читать поле можно напрямую. Снаружи его читает Current, и для этого
	// заведён отдельный замок: sortedMapMu удерживается во время блокирующей
	// отправки в out, и читатели ждали бы всё время backpressure.
	current     T
	currentMu   sync.RWMutex
	out         chan T
	sortedMap   map[T]bool
	sortedMapMu sync.Mutex
	onlyLast    bool
}

func (m *MinimalId[T]) setCurrent(v T) {
	m.currentMu.Lock()
	m.current = v
	m.currentMu.Unlock()
}

func NewMinimalId[T constraints.Integer](current T) *MinimalId[T] {
	return &MinimalId[T]{
		current:   current,
		out:       make(chan T),
		sortedMap: make(map[T]bool),
	}
}

func NewLastId[T constraints.Integer](current T) *MinimalId[T] {
	return &MinimalId[T]{
		current:   current,
		out:       make(chan T),
		sortedMap: make(map[T]bool),
		onlyLast:  true,
	}
}

func (m *MinimalId[T]) Add(new T) T {
	m.sortedMapMu.Lock()
	defer m.sortedMapMu.Unlock()
	m.sortedMap[new] = true
	_, ok := m.sortedMap[m.current+1]
	for ok {
		delete(m.sortedMap, m.current+1)
		m.setCurrent(m.current + 1)
		if !m.onlyLast {
			m.out <- m.current
		}
		_, ok = m.sortedMap[m.current+1]
		if !ok && m.onlyLast {
			m.out <- m.current
		}
	}
	return new
}

// Forward переносит позицию вперёд, отбрасывая всё, что осталось ниже неё.
// Нужен, когда часть очереди уже вырезана и ждать пропущенные id бессмысленно:
// без этого current никогда не дойдёт до следующего существующего id и цепочка
// встанет навсегда. Двигает только вперёд.
func (m *MinimalId[T]) Forward(to T) {
	m.sortedMapMu.Lock()
	defer m.sortedMapMu.Unlock()
	if to <= m.current {
		return
	}
	for id := range m.sortedMap {
		if id <= to {
			delete(m.sortedMap, id)
		}
	}
	m.setCurrent(to)
}

func (m *MinimalId[T]) C() chan T {
	return m.out
}

func (m *MinimalId[T]) Current() T {
	m.currentMu.RLock()
	defer m.currentMu.RUnlock()
	return m.current
}
