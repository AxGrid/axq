package service

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axtransform"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewReaderTransformer(t *testing.T) {
	ctx := context.Background()
	outChan := make(chan domain.Message, 1)
	outChan <- &messageHolder{
		id:      1,
		fid:     1,
		message: []byte("message"),
		ack:     make(chan blobAck, 1),
	}
	msg := <-outChan

	middlewares := []TransformMiddlewareFunc[string]{
		func(ctx *axtransform.TransformContext[domain.Message, string]) {
			if len(ctx.From.Message()) == 0 {
				ctx.Error()
				ctx.Abort()
			}
		},
		func(ctx *axtransform.TransformContext[domain.Message, string]) {
			ctx.To = string(ctx.From.Message())
			ctx.Next()
		},
		func(ctx *axtransform.TransformContext[domain.Message, string]) {
			ctx.Next()
			if ctx.Error() == nil {
				ctx.From.Done()
			} else {
				ctx.From.Error(ctx.Error())
			}
		},
	}

	transformer := NewReaderTransformer[string]().
		WithContext(ctx).
		WithMiddlewares(middlewares...).
		Build()

	newMsg := <-transformer.C()
	assert.Equal(t, newMsg.Data(), "message")

	tr, err := transformer.Transform(msg)
	assert.Nil(t, err)
	fmt.Println(tr)
}
