package axq

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/service"
	"github.com/axgrid/axq/utils"
	"github.com/rs/zerolog"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type WriterTransformer[T any] interface {
	Push(message T) error
	PushMany(messages []T) error
	Close()
}

type WriterTransformerBuilder[T any] struct {
	opts       domain.WriterTransformerOptions[T]
	dbName     string
	dbUser     string
	dbPassword string
	dbHost     string
	dbPort     int
}

func WriterTransformerBuild[T any]() *WriterTransformerBuilder[T] {
	ctx := context.Background()
	return &WriterTransformerBuilder[T]{
		dbUser:     "root",
		dbPassword: "",
		dbHost:     "localhost",
		dbPort:     3306,
		dbName:     "axq",
		opts: domain.WriterTransformerOptions[T]{
			WriterOptions: domain.WriterOptions{
				BaseOptions: domain.BaseOptions{
					CTX:    ctx,
					Logger: zerolog.Nop(),
					Name:   "unnamed",
				},
				MaxBlobSize:     10_000,
				PartitionsCount: 4,
				DB: domain.DataBaseOptions{
					Compression: domain.CompressionOptions{
						Compression: domain.BLOB_COMPRESSION_GZIP,
					},
				},
			},
		},
	}
}

func (b *WriterTransformerBuilder[T]) WithName(name string) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.Name = name
	return b
}

func (b *WriterTransformerBuilder[T]) WithLogger(logger zerolog.Logger) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.BaseOptions.Logger = logger
	return b
}

func (b *WriterTransformerBuilder[T]) WithContext(ctx context.Context) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.BaseOptions.CTX = ctx
	return b
}

func (b *WriterTransformerBuilder[T]) WithDB(db *gorm.DB) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.DB.DB = db
	return b
}
func (b *WriterTransformerBuilder[T]) WithoutCompression() *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.DB.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}
func (b *WriterTransformerBuilder[T]) WithEncryption(key []byte) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.DB.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.WriterOptions.DB.Compression.EncryptionKey = key
	return b
}
func (b *WriterTransformerBuilder[T]) WithMaxBlobSize(size int) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.MaxBlobSize = size
	return b
}

func (b *WriterTransformerBuilder[T]) WithPartitionsCount(count int) *WriterTransformerBuilder[T] {
	b.opts.WriterOptions.PartitionsCount = count
	return b
}

func (b *WriterTransformerBuilder[T]) WithMiddlewares(middlewares ...domain.WriterTransformMiddlewareFunc[T]) *WriterTransformerBuilder[T] {
	b.opts.Middlewares = append(b.opts.Middlewares, middlewares...)
	return b
}

func (b *WriterTransformerBuilder[F]) Build() (WriterTransformer[F], error) {
	if b.opts.DB.DB == nil {
		gLogger := utils.NewGLogger(b.opts.Logger, true).LogMode(logger.Warn)
		connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", b.dbUser, b.dbPassword, b.dbHost, b.dbPort, b.dbName)
		db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			return nil, err
		}
		b.opts.DB.DB = db
	}
	res, err := service.NewWriterTransformer[F](b.opts)
	if err != nil {
		return nil, err
	}
	return res, nil
}
