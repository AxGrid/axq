package axtransform

import (
	"context"
	"github.com/rs/zerolog"
)

/*
 __    _           ___
|  |  |_|_____ ___|_  |
|  |__| |     | .'|  _|
|_____|_|_|_|_|__,|___|
zed (21.03.2024)
*/

type AxTransform[F, T any] struct {
	logger      *zerolog.Logger
	ctx         context.Context
	middlewares TransformChain[F, T]
}

type Builder[F, T any] struct {
	logger      *zerolog.Logger
	ctx         context.Context
	middlewares TransformChain[F, T]
}

func NewAxTransform[F, T any]() *Builder[F, T] {
	return &Builder[F, T]{
		ctx: context.Background(),
	}
}

func (b *Builder[F, T]) WithContext(ctx context.Context) *Builder[F, T] {
	b.ctx = ctx
	return b
}

func (b *Builder[F, T]) WithMiddlewares(middlewares ...TransformFunc[F, T]) *Builder[F, T] {
	b.middlewares = append(b.middlewares, middlewares...)
	return b
}

func (b *Builder[F, T]) WithLogger(logger zerolog.Logger) *Builder[F, T] {
	b.logger = &logger
	return b
}

func (b *Builder[F, T]) Build() *AxTransform[F, T] {
	return &AxTransform[F, T]{
		logger:      b.logger,
		ctx:         b.ctx,
		middlewares: b.middlewares,
	}
}

func (t *AxTransform[F, T]) Transform(from F) (T, error) {
	ctx := &TransformContext[F, T]{
		ctx:   t.ctx,
		index: -1,
	}
	ctx.logger = t.logger
	ctx.From = from
	ctx.handlers = t.middlewares
	ctx.Next()
	return ctx.To, ctx.err
}
