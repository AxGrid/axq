package axq

import (
	"context"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/service"
	"github.com/rs/zerolog"
	"gopkg.in/kothar/go-backblaze.v0"
)

type B2Reader interface {
	Pop() domain.Message
	C() <-chan domain.Message
}

type B2ReaderBuilder struct {
	opts domain.B2ReaderOptions
}

func B2ReaderBuild() *B2ReaderBuilder {
	return &B2ReaderBuilder{
		opts: domain.B2ReaderOptions{
			BaseOptions: domain.BaseOptions{
				CTX:    context.Background(),
				Logger: zerolog.Nop(),
				Name:   "unnamed",
			},
			LoaderCount: 2,
			OuterCount:  2,
			BufferSize:  100_000,
		},
	}
}

func (b *B2ReaderBuilder) WithName(name string) *B2ReaderBuilder {
	b.opts.Name = name
	return b
}

func (b *B2ReaderBuilder) WithLogger(logger zerolog.Logger) *B2ReaderBuilder {
	b.opts.Logger = logger
	return b
}

func (b *B2ReaderBuilder) WithContext(ctx context.Context) *B2ReaderBuilder {
	b.opts.CTX = ctx
	return b
}

func (b *B2ReaderBuilder) WithLoaderCount(loaderCount int) *B2ReaderBuilder {
	b.opts.LoaderCount = loaderCount
	return b
}

func (b *B2ReaderBuilder) WithBufferSize(bufferSize int) *B2ReaderBuilder {
	b.opts.BufferSize = bufferSize
	return b
}

func (b *B2ReaderBuilder) WithOuterCount(waiterCount int) *B2ReaderBuilder {
	b.opts.OuterCount = waiterCount
	return b
}

func (b *B2ReaderBuilder) WithB2Credentials(credentials backblaze.Credentials) *B2ReaderBuilder {
	b.opts.B2.Credentials = credentials
	return b
}

func (b *B2ReaderBuilder) WithB2Salt(salt string) *B2ReaderBuilder {
	b.opts.B2.Salt = salt
	return b
}

func (b *B2ReaderBuilder) WithoutCompression() *B2ReaderBuilder {
	b.opts.B2.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}

func (b *B2ReaderBuilder) WithEncryption(key []byte) *B2ReaderBuilder {
	b.opts.B2.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.B2.Compression.EncryptionKey = key
	return b
}

func (b *B2ReaderBuilder) WithReaderName(name string) *B2ReaderBuilder {
	b.opts.ReaderName = name
	return b
}

func (b *B2ReaderBuilder) WithLastId(id *domain.LastIdOptions) *B2ReaderBuilder {
	b.opts.LastId = id
	return b
}

func (b *B2ReaderBuilder) Build() (B2Reader, error) {
	res, err := service.NewB2ReaderService(b.opts)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *B2ReaderBuilder) ShouldBuild() B2Reader {
	res, err := b.Build()
	if err != nil {
		panic(err)
	}
	return res
}
