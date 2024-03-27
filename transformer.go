package axq

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/service"
	"github.com/axgrid/axq/utils"
	"github.com/rs/zerolog"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Transformer[T any] interface {
	Transform(from domain.Message) (T, error)
}

type ReaderTransformerBuilder[T any] struct {
	opts        domain.ReaderTransformerOptions[T]
	workerCount int
	workerFunc  domain.TransformWorkerFunc[T]
	dbName      string
	dbUser      string
	dbPassword  string
	dbHost      string
	dbPort      int
}

func ReaderTransformerBuild[T any]() *ReaderTransformerBuilder[T] {
	ctx := context.Background()
	return &ReaderTransformerBuilder[T]{
		dbName:     "axq",
		dbUser:     "root",
		dbPassword: "",
		dbHost:     "localhost",
		dbPort:     3306,
		opts: domain.ReaderTransformerOptions[T]{
			ReaderOptions: domain.ReaderOptions{
				BaseOptions: domain.BaseOptions{
					CTX:    ctx,
					Logger: zerolog.Nop(),
					Name:   "unnamed",
				},
				DB: domain.DataBaseOptions{
					Compression: domain.CompressionOptions{
						Compression: domain.BLOB_COMPRESSION_GZIP,
					},
				},
				BatchSize:   10,
				LoaderCount: 10,
				BufferSize:  5_000_000,
				WaiterCount: 10,
			},
		},
	}
}

func (b *ReaderTransformerBuilder[T]) WithName(name string) *ReaderTransformerBuilder[T] {
	b.opts.Name = name
	return b
}

func (b *ReaderTransformerBuilder[T]) WithLogger(logger zerolog.Logger) *ReaderTransformerBuilder[T] {
	b.opts.Logger = logger
	return b
}

func (b *ReaderTransformerBuilder[T]) WithContext(ctx context.Context) *ReaderTransformerBuilder[T] {
	b.opts.ReaderOptions.CTX = ctx
	return b
}

func (b *ReaderTransformerBuilder[T]) WithLoaderCount(loaderCount int) *ReaderTransformerBuilder[T] {
	b.opts.LoaderCount = loaderCount
	return b
}

func (b *ReaderTransformerBuilder[T]) WithBufferSize(bufferSize int) *ReaderTransformerBuilder[T] {
	b.opts.BufferSize = bufferSize
	return b
}

func (b *ReaderTransformerBuilder[T]) WithWaiterCount(waiterCount int) *ReaderTransformerBuilder[T] {
	b.opts.WaiterCount = waiterCount
	return b
}

func (b *ReaderTransformerBuilder[T]) WithB2Credentials(credentials backblaze.Credentials) *ReaderTransformerBuilder[T] {
	b.opts.B2.Credentials = credentials
	return b
}

func (b *ReaderTransformerBuilder[T]) WithB2Salt(salt string) *ReaderTransformerBuilder[T] {
	b.opts.B2.Salt = salt
	return b
}

func (b *ReaderTransformerBuilder[T]) WithDB(db *gorm.DB) *ReaderTransformerBuilder[T] {
	b.opts.DB.DB = db
	return b
}

func (b *ReaderTransformerBuilder[T]) WithoutCompression() *ReaderTransformerBuilder[T] {
	b.opts.DB.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}

func (b *ReaderTransformerBuilder[T]) WithEncryption(key []byte) *ReaderTransformerBuilder[T] {
	b.opts.DB.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.DB.Compression.EncryptionKey = key
	return b
}

func (b *ReaderTransformerBuilder[T]) WithReaderName(name string) *ReaderTransformerBuilder[T] {
	b.opts.ReaderName = name
	return b
}

func (b *ReaderTransformerBuilder[T]) WithLastId(id *domain.LastIdOptions) *ReaderTransformerBuilder[T] {
	b.opts.LastId = id
	return b
}

func (b *ReaderTransformerBuilder[T]) WithWorkerFunc(count int, f domain.TransformWorkerFunc[T]) *ReaderTransformerBuilder[T] {
	b.workerCount = count
	b.workerFunc = f
	return b
}

func (b *ReaderTransformerBuilder[T]) WithMiddlewares(middlewares ...domain.TransformMiddlewareFunc[T]) *ReaderTransformerBuilder[T] {
	b.opts.Middlewares = append(b.opts.Middlewares, middlewares...)
	return b
}

func (b *ReaderTransformerBuilder[T]) Build() (Transformer[T], error) {
	if b.opts.DB.DB == nil {
		gLogger := utils.NewGLogger(b.opts.Logger, true).LogMode(logger.Warn)
		connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", b.dbUser, b.dbPassword, b.dbHost, b.dbPort, b.dbName)
		db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			return nil, err
		}
		b.opts.DB.DB = db
	}

	res, err := service.NewReaderTransformer[T](b.opts)
	if err != nil {
		return nil, err
	}
	if b.workerFunc != nil {
		for i := 0; i < b.workerCount; i++ {
			go func(i int) {
				select {
				case <-b.opts.BaseOptions.CTX.Done():
					return
				case msg := <-res.C():
					b.workerFunc(i, msg)
				}
			}(i)
		}
	}
	return res, nil
}
