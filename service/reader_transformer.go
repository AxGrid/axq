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

type ReaderTransformerBuilder[T any] struct {
	ctx         context.Context
	middlewares []axtransform.TransformFunc[domain.Message, T]
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

func (b *ReaderTransformerBuilder[T]) WithMiddlewares(middlewares ...axtransform.TransformFunc[domain.Message, T]) *ReaderTransformerBuilder[T] {
	b.middlewares = append(b.middlewares, middlewares...)
	return b
}

func (b *ReaderTransformerBuilder[T]) Build() *ReaderTransformer[T] {
	return &ReaderTransformer[T]{
		ctx:         b.ctx,
		middlewares: b.middlewares,
		transformer: b.builder.WithMiddlewares(b.middlewares...).Build(),
	}
}

func (t *ReaderTransformer[T]) Transform(from domain.Message) (T, error) {
	return t.transformer.Transform(from)
}
