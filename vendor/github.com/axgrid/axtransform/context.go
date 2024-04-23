package axtransform

import (
	"context"
	"github.com/rs/zerolog"
	"math"
)

/*
 __    _           ___
|  |  |_|_____ ___|_  |
|  |__| |     | .'|  _|
|_____|_|_|_|_|__,|___|
zed (21.03.2024)
*/

const abortIndex int8 = math.MaxInt8 >> 1

type TransformFunc[F, T any] func(*TransformContext[F, T])
type TransformChain[F, T any] []TransformFunc[F, T]

type TransformContext[F, T any] struct {
	From     F
	logger   *zerolog.Logger
	To       T
	err      error
	ctx      context.Context
	index    int8
	handlers TransformChain[F, T]
}

func (c *TransformContext[F, T]) Context() context.Context {
	return c.ctx
}

func (c *TransformContext[F, T]) Logger() *zerolog.Logger {
	if c.logger != nil {
		return c.logger
	}
	//GetFromContext
	c.logger = c.Value("logger").(*zerolog.Logger)
	if c.logger == nil {
		logger := zerolog.Nop()
		c.logger = &logger
	}
	return c.logger
}

func (c *TransformContext[F, T]) Value(name any, def ...interface{}) interface{} {
	res := c.ctx.Value(name)
	if res == nil && len(def) > 0 {
		return def[0]
	}
	return res
}

func (c *TransformContext[F, T]) WithValue(name any, value interface{}) *TransformContext[F, T] {
	c.ctx = context.WithValue(c.ctx, name, value)
	return c
}

func (c *TransformContext[F, T]) Error() error {
	return c.err
}

func (c *TransformContext[R, S]) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		c.handlers[c.index](c)
		c.index++
	}
}

func (c *TransformContext[R, S]) Abort() {
	c.index = abortIndex
}

func (c *TransformContext[R, S]) IsAborted() bool {
	return c.index >= abortIndex
}
