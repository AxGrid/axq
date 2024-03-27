package domain

import "github.com/axgrid/axtransform"

type TransformHolder[T any] interface {
	Message() Message
	Data() T
	Done()
	Error(err error)
	Worker() int
}

type TransformMiddlewareFunc[T any] func(ctx *axtransform.TransformContext[Message, T])

type TransformFunc[T any] func(worker int, t TransformHolder[T])
