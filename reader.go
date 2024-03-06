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

type ReaderBuilder struct {
	opts       domain.ReaderOptions
	dbName     string
	dbUser     string
	dbPassword string
	dbHost     string
	dbPort     int
}

func Reader() *ReaderBuilder {
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
			LoaderCount: 50,
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

func (b *ReaderBuilder) Build() (domain.Reader, error) {
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
	return res, nil
}

func (b *ReaderBuilder) ShouldBuild() domain.Reader {
	res, err := b.Build()
	if err != nil {
		panic(err)
	}
	return res
}
