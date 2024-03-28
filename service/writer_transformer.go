package service

import (
	"context"
	"errors"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
)

type WriterTransformer[F any] struct {
	ctx         context.Context
	transformer *axtransform.AxTransform[F, []byte]
	writer      *WriterService
}

func NewWriterTransformer[F any](opts domain.WriterTransformerOptions[F]) (*WriterTransformer[F], error) {
	middlewaresFunc := make([]axtransform.TransformFunc[F, []byte], 0, len(opts.Middlewares))
	for _, m := range opts.Middlewares {
		middlewaresFunc = append(middlewaresFunc, func(ctx *axtransform.TransformContext[F, []byte]) {
			m(ctx)
		})
	}
	transformer := axtransform.NewAxTransform[F, []byte]().
		WithContext(opts.WriterOptions.BaseOptions.CTX).
		WithMiddlewares(middlewaresFunc...).
		Build()
	w, err := NewWriterService(opts.WriterOptions)
	if err != nil {
		return nil, err
	}

	return &WriterTransformer[F]{
		ctx:         opts.WriterOptions.BaseOptions.CTX,
		transformer: transformer,
		writer:      w,
	}, nil
}

func (w *WriterTransformer[F]) Push(message F) error {
	if w.writer.stopped {
		return errors.New("writer stopped")
	}
	tr, err := w.transformer.Transform(message)
	if err != nil {
		return err
	}
	holder := &dataHolder{
		message:  tr,
		response: make(chan error, 1),
	}
	w.writer.inChan <- holder
	return <-holder.response
}

func (w *WriterTransformer[F]) PushMany(messages []F) error {
	if w.writer.stopped {
		return errors.New("writer stopped")
	}
	var holders = make([]*dataHolder, len(messages))
	for i, message := range messages {
		tr, err := w.transformer.Transform(message)
		if err != nil {
			return err
		}
		holder := &dataHolder{
			message:  tr,
			response: make(chan error, 1),
		}
		holders[i] = holder
		w.writer.inChan <- holder
	}
	for _, holder := range holders {
		if err := <-holder.response; err != nil {
			return err
		}
	}
	return nil
}

func (w *WriterTransformer[F]) Close() {
	w.writer.Close()
}
