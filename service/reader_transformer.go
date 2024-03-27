package service

import (
	"context"
	"errors"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
)

type ReaderTransformer[T any] struct {
	ctx         context.Context
	transformer *axtransform.AxTransform[domain.Message, T]
	outChan     chan domain.TransformHolder[T]
	reader      *ReaderService
}

func (r *ReaderTransformer[T]) C() <-chan domain.TransformHolder[T] {
	return r.outChan
}

func NewReaderTransformer[T any](opts domain.ReaderTransformerOptions[T]) (*ReaderTransformer[T], error) {
	middlewaresFunc := make([]axtransform.TransformFunc[domain.Message, T], 0, len(opts.Middlewares))
	for _, m := range opts.Middlewares {
		middlewaresFunc = append(middlewaresFunc, func(ctx *axtransform.TransformContext[domain.Message, T]) {
			m(ctx)
		})
	}
	transformer := axtransform.NewAxTransform[domain.Message, T]().
		WithMiddlewares(middlewaresFunc...).
		Build()

	reader, err := NewReaderService(opts.ReaderOptions)
	if err != nil {
		return nil, err
	}
	res := &ReaderTransformer[T]{
		ctx:         opts.ReaderOptions.BaseOptions.CTX,
		transformer: transformer,
		outChan:     make(chan domain.TransformHolder[T]),
		reader:      reader,
	}
	go func() {
		for {
			select {
			case msg := <-res.reader.C():
				t, err := res.Transform(msg)
				if errors.Is(err, domain.ErrSkipMessage) {
					msg.Done()
					continue
				}
				if err != nil {
					msg.Error(err)
					continue
				}
				holder := &transformHolder[T]{
					msg:  msg,
					data: t,
				}
				res.outChan <- holder
			}
		}
	}()
	return res, nil
}

func (t *ReaderTransformer[T]) Transform(from domain.Message) (T, error) {
	return t.transformer.Transform(from)
}

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
