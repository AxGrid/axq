package service

import (
	"context"
	"errors"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
)

type TransformMiddlewareFunc[T any] func(ctx *axtransform.TransformContext[domain.Message, T])

type ReaderTransformer[T any] struct {
	ctx         context.Context
	transformer *axtransform.AxTransform[domain.Message, T]
	outChan     chan TransformHolder[T]
	reader      *ReaderService
}

func (r *ReaderTransformer[T]) Pop() domain.Message {
	return nil
}

func (r *ReaderTransformer[T]) GetOpts() domain.ServiceOpts {
	return r.reader.GetOpts()
}

func (r *ReaderTransformer[T]) Counter() (uint64, error) {
	return r.reader.Counter()
}

func (r *ReaderTransformer[T]) LastFID() (uint64, error) {
	return r.reader.LastFID()
}

func (r *ReaderTransformer[T]) LastID() (uint64, error) {
	return r.reader.LastID()
}

func (r *ReaderTransformer[T]) Performance() uint64 {
	return r.reader.Performance()
}

type ReaderTransformerBuilder[T any] struct {
	ctx         context.Context
	middlewares []TransformMiddlewareFunc[T]
	builder     *axtransform.Builder[domain.Message, T]
	reader      *ReaderService
}

func (r *ReaderTransformer[T]) C() <-chan TransformHolder[T] {
	return r.outChan
}

func NewReaderTransformer[T any]() *ReaderTransformerBuilder[T] {
	return &ReaderTransformerBuilder[T]{
		builder: axtransform.NewAxTransform[domain.Message, T](),
	}
}

func (b *ReaderTransformerBuilder[T]) WithContext(ctx context.Context) *ReaderTransformerBuilder[T] {
	b.ctx = ctx
	return b
}

func (b *ReaderTransformerBuilder[T]) WithMiddlewares(middlewares ...TransformMiddlewareFunc[T]) *ReaderTransformerBuilder[T] {
	b.middlewares = append(b.middlewares, middlewares...)
	return b
}

func (b *ReaderTransformerBuilder[T]) WithReader(r *ReaderService) *ReaderTransformerBuilder[T] {
	b.reader = r
	return b
}

func (b *ReaderTransformerBuilder[T]) Build() *ReaderTransformer[T] {
	middlewaresFunc := make([]axtransform.TransformFunc[domain.Message, T], 0, len(b.middlewares))
	for _, m := range b.middlewares {
		middlewaresFunc = append(middlewaresFunc, func(ctx *axtransform.TransformContext[domain.Message, T]) {
			m(ctx)
		})
	}
	res := &ReaderTransformer[T]{
		ctx:         b.ctx,
		transformer: b.builder.WithMiddlewares(middlewaresFunc...).Build(),
		outChan:     make(chan TransformHolder[T]),
		reader:      b.reader,
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
	return res
}

func (t *ReaderTransformer[T]) Transform(from domain.Message) (T, error) {
	return t.transformer.Transform(from)
}
