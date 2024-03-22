package service

import (
	"context"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
)

type ReaderTransformer[T any] struct {
	ctx         context.Context
	middlewares []axtransform.TransformFunc[domain.Message, T]
	transformer *axtransform.AxTransform[domain.Message, T]
}

type TransformMiddlewareFunc[T any] func(ctx *axtransform.TransformContext[domain.Message, T])

type ReaderTransformerBuilder[T any] struct {
	ctx         context.Context
	middlewares []TransformMiddlewareFunc[T]
	builder     *axtransform.Builder[domain.Message, T]
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

func (b *ReaderTransformerBuilder[T]) Build() *ReaderTransformer[T] {
	middlewaresFunc := make([]axtransform.TransformFunc[domain.Message, T], 0, len(b.middlewares))
	for _, m := range b.middlewares {
		middlewaresFunc = append(middlewaresFunc, func(ctx *axtransform.TransformContext[domain.Message, T]) {
			m(ctx)
		})
	}
	return &ReaderTransformer[T]{
		ctx:         b.ctx,
		middlewares: middlewaresFunc,
		transformer: b.builder.WithMiddlewares(middlewaresFunc...).Build(),
	}
}

func (t *ReaderTransformer[T]) Transform(from domain.Message) (T, error) {
	return t.transformer.Transform(from)
}
