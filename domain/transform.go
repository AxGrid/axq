package domain

import "github.com/axgrid/axtransform"

type TransformHolder[T any] interface {
	Message() Message
	Data() T
	Done()
	Error(err error)
	Worker() int
}

type WorkerFunc func(i int, msg Message)

type TransformWorkerFunc[T any] func(i int, T any)

type TransformMiddlewareFunc[T any] func(ctx *axtransform.TransformContext[Message, T])

type TransformFunc[T any] func(worker int, t TransformHolder[T])
