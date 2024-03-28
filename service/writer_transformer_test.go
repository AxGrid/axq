package service

import (
	"context"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriterTransformer_Push(t *testing.T) {
	middlewares := []domain.WriterTransformMiddlewareFunc[string]{
		func(ctx *axtransform.TransformContext[string, []byte]) {
			ctx.Next()
		},
		func(ctx *axtransform.TransformContext[string, []byte]) {
			ctx.To = []byte(ctx.From)
			ctx.Next()
		},
	}

	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test_new",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB:          testDataBase,
			Compression: domain.CompressionOptions{},
		},
		PartitionsCount: 4,
		MaxBlobSize:     100000,
	}

	transformOpts := domain.WriterTransformerOptions[string]{
		WriterOptions: opts,
		Middlewares:   middlewares,
	}

	transform, err := NewWriterTransformer[string](transformOpts)
	if err != nil {
		panic(err)
	}

	msg := "message"
	err = transform.Push(msg)
	assert.Nil(t, err)

}

func TestWriterTransformer_PushMany(t *testing.T) {
	middlewares := []domain.WriterTransformMiddlewareFunc[string]{
		func(ctx *axtransform.TransformContext[string, []byte]) {
			ctx.Next()
		},
		func(ctx *axtransform.TransformContext[string, []byte]) {
			ctx.To = []byte(ctx.From)
			ctx.Next()
		},
	}

	opts := domain.WriterOptions{
		BaseOptions: domain.BaseOptions{
			Name:   "test_new",
			Logger: log.Logger,
			CTX:    context.Background(),
		},
		DB: domain.DataBaseOptions{
			DB:          testDataBase,
			Compression: domain.CompressionOptions{},
		},
		PartitionsCount: 4,
		MaxBlobSize:     100000,
	}

	transformOpts := domain.WriterTransformerOptions[string]{
		WriterOptions: opts,
		Middlewares:   middlewares,
	}

	transform, err := NewWriterTransformer[string](transformOpts)
	if err != nil {
		panic(err)
	}

	msgs := []string{"message1", "message2", "message3", "message4"}
	err = transform.PushMany(msgs)
	assert.Nil(t, err)
}
