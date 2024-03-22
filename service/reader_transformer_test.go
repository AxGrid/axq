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

	middlewares := []axtransform.TransformFunc[domain.Message, *messageHolder]{
		func(t *axtransform.TransformContext[domain.Message, *messageHolder]) {
			holder := t.From.(*messageHolder)
			holder.fid++
			t.To = holder
		},
	}

	transformer := NewReaderTransformer[*messageHolder]().
		WithContext(ctx).
		WithMiddlewares(middlewares...).
		Build()

	tr, err := transformer.Transform(msg)
	assert.Nil(t, err)
	fmt.Println(tr)
}
