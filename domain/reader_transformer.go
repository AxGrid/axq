package domain

import (
	"context"
	"github.com/axgrid/axq"
	"github.com/axgrid/axtransform"
)

type ReaderTransformer[T any] struct {
	ctx         context.Context
	reader      axq.Reader
	transformer *axtransform.AxTransform[axq.ReaderMessageHolder[axq.Message], T]
}

type ReaderTransformerBuilder[T any] struct {
	ctx     context.Context
	builder *axtransform.Builder[axq.ReaderMessageHolder[axq.Message], T]
}

func NewReaderTransformer[T any]() *ReaderTransformerBuilder[T] {
	return &ReaderTransformerBuilder[T]{
		builder: axtransform.NewAxTransform[axq.ReaderMessageHolder[axq.Message], T](),
	}
}

func (b *ReaderTransformerBuilder[T]) WithContext(ctx context.Context) *ReaderTransformerBuilder[T] {
	b.ctx = ctx
	return b
}

func (b *ReaderTransformerBuilder[T]) WithMiddlewares(middlewares ...TransformFunc[T]) *ReaderTransformerBuilder[T] {
	return b
}

func (b *ReaderTransformerBuilder[T]) Build(r axq.Reader) *ReaderTransformer[T] {
	tr := axtransform.NewAxTransform[axq.ReaderMessageHolder[axq.Message], T]().Build()
	return &ReaderTransformer[T]{
		ctx:         b.ctx,
		reader:      r,
		transformer: tr,
	}
}

//func (t *ReaderTransformer[T]) Transform(from F) (T, error) {
//	return nil, nil
//}
