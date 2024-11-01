/*
 * Created by Zed 08.12.2023, 14:39
 */

package axq

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/service"
	"github.com/axgrid/axq/utils"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"time"
)

type Writer interface {
	Push(message []byte) error
	PushMany(messages [][]byte) error
	PushProto(message proto.Message) error
	PushProtoMany(messages []proto.Message) error
	Close()
	GetOpts() domain.ServiceOpts
	Counter() (uint64, error)
	LastFID() (uint64, error)
	LastID() (uint64, error)
	MinimalFID() (uint64, error)
	MinimalID() (uint64, error)
	Performance() uint64
}

type WriterBuilder struct {
	opts       domain.WriterOptions
	dbName     string
	dbUser     string
	dbPassword string
	dbHost     string
	dbPort     int
}

func NewWriter() *WriterBuilder {
	return &WriterBuilder{
		dbUser:     "root",
		dbPassword: "",
		dbHost:     "localhost",
		dbPort:     3306,
		dbName:     "axq",
		opts: domain.WriterOptions{
			BaseOptions: domain.BaseOptions{
				CTX:    context.Background(),
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
	}
}

func (b *WriterBuilder) WithName(name string) *WriterBuilder {
	b.opts.Name = name
	return b
}

func (b *WriterBuilder) WithLogger(logger zerolog.Logger) *WriterBuilder {
	b.opts.Logger = logger
	return b
}

func (b *WriterBuilder) WithContext(ctx context.Context) *WriterBuilder {
	b.opts.CTX = ctx
	return b
}

func (b *WriterBuilder) WithDB(db *gorm.DB) *WriterBuilder {
	b.opts.DB.DB = db
	return b
}
func (b *WriterBuilder) WithoutCompression() *WriterBuilder {
	b.opts.DB.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}
func (b *WriterBuilder) WithEncryption(key []byte) *WriterBuilder {
	b.opts.DB.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.DB.Compression.EncryptionKey = key
	return b
}
func (b *WriterBuilder) WithMaxBlobSize(size int) *WriterBuilder {
	b.opts.MaxBlobSize = size
	return b
}

func (b *WriterBuilder) WithPartitionsCount(count int) *WriterBuilder {
	b.opts.PartitionsCount = count
	return b
}

func (b *WriterBuilder) WithCutFrequency(t time.Duration) *WriterBuilder {
	b.opts.CutFrequency = t
	return b
}

func (b *WriterBuilder) WithCutSize(size int) *WriterBuilder {
	b.opts.CutSize = size
	return b
}

func (b *WriterBuilder) Build() (Writer, error) {
	if b.opts.DB.DB == nil {
		gLogger := utils.NewGLogger(b.opts.Logger, true).LogMode(logger.Warn)
		connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", b.dbUser, b.dbPassword, b.dbHost, b.dbPort, b.dbName)
		db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			return nil, err
		}
		b.opts.DB.DB = db
	}
	res, err := service.NewWriterService(b.opts)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (b *WriterBuilder) ShouldBuild() Writer {
	res, err := b.Build()
	if err != nil {
		panic(err)
	}
	return res
}
