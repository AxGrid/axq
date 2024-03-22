package service

import (
	"github.com/axgrid/axq/domain"
)

type transformHolder[T any] struct {
	worker int
	msg    domain.Message
	data   T
}

func (t *transformHolder[T]) Message() domain.Message {
	return t.msg
}
func (t *transformHolder[T]) Done() {
	t.msg.Done()
}

func (t *transformHolder[T]) Error(err error) {
	t.msg.Error(err)
}

func (t *transformHolder[T]) Data() T {
	return t.data
}

func (t *transformHolder[T]) Worker() int {
	return t.worker
}

type TransformHolder[T any] interface {
	Message() domain.Message
	Data() T
	Done()
	Error(err error)
	Worker() int
}

type TransformFunc[T any] func(worker int, t TransformHolder[T])
