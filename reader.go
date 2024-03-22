/*
 * Created by Zed 10.12.2023, 10:19
 */

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

type Reader interface {
	Pop() Message
	C() <-chan Message
	GetOpts() domain.ServiceOpts
	Counter() (uint64, error)
	LastFID() (uint64, error)
	LastID() (uint64, error)
	Performance() uint64
}

type ReaderBuilder struct {
	opts               domain.ReaderOptions
	dbName             string
	dbUser             string
	dbPassword         string
	dbHost             string
	dbPort             int
	workerCount        int
	workerFunc         WorkerFunc
	transformer        *domain.ReaderTransformer[any]
	transformFunctions []domain.TransformFunc[any]
}

func ReaderBuild() *ReaderBuilder {
	return &ReaderBuilder{
		dbName:     "axq",
		dbUser:     "root",
		dbPassword: "",
		dbHost:     "localhost",
		dbPort:     3306,
		opts: domain.ReaderOptions{
			BaseOptions: domain.BaseOptions{
				CTX:    context.Background(),
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
	}
}

func (b *ReaderBuilder) WithName(name string) *ReaderBuilder {
	b.opts.Name = name
	return b
}

func (b *ReaderBuilder) WithLogger(logger zerolog.Logger) *ReaderBuilder {
	b.opts.Logger = logger
	return b
}

func (b *ReaderBuilder) WithContext(ctx context.Context) *ReaderBuilder {
	b.opts.CTX = ctx
	return b
}

func (b *ReaderBuilder) WithLoaderCount(loaderCount int) *ReaderBuilder {
	b.opts.LoaderCount = loaderCount
	return b
}

func (b *ReaderBuilder) WithBufferSize(bufferSize int) *ReaderBuilder {
	b.opts.BufferSize = bufferSize
	return b
}

func (b *ReaderBuilder) WithWaiterCount(waiterCount int) *ReaderBuilder {
	b.opts.WaiterCount = waiterCount
	return b
}

func (b *ReaderBuilder) WithB2Credentials(credentials backblaze.Credentials) *ReaderBuilder {
	b.opts.B2.Credentials = credentials
	return b
}

func (b *ReaderBuilder) WithB2Salt(salt string) *ReaderBuilder {
	b.opts.B2.Salt = salt
	return b
}

func (b *ReaderBuilder) WithDB(db *gorm.DB) *ReaderBuilder {
	b.opts.DB.DB = db
	return b
}

func (b *ReaderBuilder) WithoutCompression() *ReaderBuilder {
	b.opts.DB.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}

func (b *ReaderBuilder) WithEncryption(key []byte) *ReaderBuilder {
	b.opts.DB.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.DB.Compression.EncryptionKey = key
	return b
}

func (b *ReaderBuilder) WithReaderName(name string) *ReaderBuilder {
	b.opts.ReaderName = name
	return b
}

func (b *ReaderBuilder) WithLastId(id *domain.LastIdOptions) *ReaderBuilder {
	b.opts.LastId = id
	return b
}

type WorkerFunc func(i int, msg Message)

func (b *ReaderBuilder) WithWorkerFunc(count int, f WorkerFunc) *ReaderBuilder {
	b.workerCount = count
	b.workerFunc = f
	return b
}

func (b *ReaderBuilder) WithTransformFunctions(functions ...domain.TransformFunc[any]) *ReaderBuilder {
	b.transformFunctions = functions
	return b
}

func (b *ReaderBuilder) Build() (Reader, error) {
	if b.opts.DB.DB == nil {
		gLogger := utils.NewGLogger(b.opts.Logger, true).LogMode(logger.Warn)
		connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", b.dbUser, b.dbPassword, b.dbHost, b.dbPort, b.dbName)
		db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			return nil, err
		}
		b.opts.DB.DB = db
	}
	res, err := service.NewReaderService(b.opts)
	if err != nil {
		return nil, err
	}
	if b.workerFunc != nil {
		//TODO transforms
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

func (b *ReaderBuilder) ShouldBuild() Reader {
	res, err := b.Build()
	if err != nil {
		panic(err)
	}
	return res
}
