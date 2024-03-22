package domain

import (
	"context"
	"github.com/axgrid/axq"
	"github.com/rs/zerolog"
)

type TransformFunc[T any] func(*TransformContext[T])
type TransformChain[T any] []TransformFunc[T]

type TransformContext[T any] struct {
	From     axq.ReaderMessageHolder[axq.Message]
	logger   *zerolog.Logger
	To       T
	err      error
	ctx      context.Context
	index    int8
	handlers TransformChain[T]
}
