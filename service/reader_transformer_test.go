package service

import (
	"context"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewReaderTransformer(t *testing.T) {
	middlewares := []domain.TransformMiddlewareFunc[string]{
		func(ctx *axtransform.TransformContext[domain.Message, string]) {
			ctx.Next()
		},
		func(ctx *axtransform.TransformContext[domain.Message, string]) {
			ctx.To = string(ctx.From.Message())
			ctx.Next()
		},
	}
	ctx := context.Background()
	opts := domain.ReaderTransformerOptions[string]{
		ReaderOptions: domain.ReaderOptions{
			BaseOptions: domain.BaseOptions{
				Name:   "test",
				Logger: log.Logger,
				CTX:    ctx,
			},
			DB: domain.DataBaseOptions{
				DB: testDataBase,
				Compression: domain.CompressionOptions{
					Compression:   domain.BLOB_COMPRESSION_GZIP,
					Encryption:    domain.BLOB_ENCRYPTION_AES,
					EncryptionKey: []byte("12345678901234567890123456789012"),
				},
			},
			WaiterCount: 1,
		},
		Middlewares: middlewares,
	}

	transformer, err := NewReaderTransformer[string](opts)
	if err != nil {
		panic(err)
	}

	transformer.reader.outChan <- &messageHolder{
		id:      1,
		fid:     1,
		message: []byte("message"),
		ack:     make(chan blobAck, 1),
	}

	newMsg := <-transformer.C()
	assert.Equal(t, newMsg.Data(), "message")

	//tr, err := transformer.Transform(newMsg)
	//assert.Nil(t, err)
	//fmt.Println(tr)
}
