package utils

import (
	"golang.org/x/exp/constraints"
	"sync"
)

type MinimalId[T constraints.Integer] struct {
	current     T
	out         chan T
	sortedMap   map[T]bool
	sortedMapMu sync.Mutex
	onlyLast    bool
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
		m.current++
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

func (m *MinimalId[T]) C() chan T {
	return m.out
}

func (m *MinimalId[T]) Current() T {
	return m.current
}
