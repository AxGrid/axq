/*
 * Created by Zed 10.12.2023, 10:42
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

type Archiver interface {
	Close()
}

type ArchiverBuilder struct {
	opts       domain.ArchiverOptions
	dbName     string
	dbUser     string
	dbPassword string
	dbHost     string
	dbPort     int
}

func NewArchiver() *ArchiverBuilder {
	return &ArchiverBuilder{
		dbName:     "axq",
		dbUser:     "root",
		dbPassword: "",
		dbHost:     "localhost",
		dbPort:     3306,
		opts: domain.ArchiverOptions{
			BaseOptions: domain.BaseOptions{
				Name:   "unnamed",
				CTX:    context.Background(),
				Logger: zerolog.Nop(),
			},
			DB: domain.DataBaseOptions{
				Compression: domain.CompressionOptions{
					Compression: domain.BLOB_COMPRESSION_GZIP,
				},
			},
			B2: domain.B2Options{
				Compression: domain.CompressionOptions{
					Compression: domain.BLOB_COMPRESSION_GZIP,
				},
				Salt: "ohShomu8abue9EiT",
			},
			MaxCount:  5_000_000,
			MaxSize:   1_000_000,
			ChunkSize: 2_000,
		},
	}
}

func (b *ArchiverBuilder) WithName(name string) *ArchiverBuilder {
	b.opts.Name = name
	return b
}

func (b *ArchiverBuilder) WithLogger(logger zerolog.Logger) *ArchiverBuilder {
	b.opts.Logger = logger
	return b
}

func (b *ArchiverBuilder) WithContext(ctx context.Context) *ArchiverBuilder {
	b.opts.CTX = ctx
	return b
}

func (b *ArchiverBuilder) WithDB(db *gorm.DB) *ArchiverBuilder {
	b.opts.DB.DB = db
	return b
}

func (b *ArchiverBuilder) WithoutDBCompression() *ArchiverBuilder {
	b.opts.DB.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}

func (b *ArchiverBuilder) WithoutB2Compression() *ArchiverBuilder {
	b.opts.B2.Compression.Compression = domain.BLOB_COMPRESSION_NONE
	return b
}

func (b *ArchiverBuilder) WithChunkSize(size int) *ArchiverBuilder {
	b.opts.ChunkSize = size
	return b
}

func (b *ArchiverBuilder) WithMaxSize(size int) *ArchiverBuilder {
	b.opts.MaxSize = size
	return b
}

func (b *ArchiverBuilder) WithMaxCount(count int) *ArchiverBuilder {
	b.opts.MaxCount = count
	return b
}

func (b *ArchiverBuilder) WithBDCompression(compression domain.BlobCompression) *ArchiverBuilder {
	b.opts.DB.Compression.Compression = compression
	return b
}

func (b *ArchiverBuilder) WithB2Compression(compression domain.BlobCompression) *ArchiverBuilder {
	b.opts.B2.Compression.Compression = compression
	return b
}

func (b *ArchiverBuilder) WithB2EncryptionAES(key []byte) *ArchiverBuilder {
	b.opts.B2.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.B2.Compression.EncryptionKey = key
	return b
}

func (b *ArchiverBuilder) WithDBEncryptionAES(key []byte) *ArchiverBuilder {
	b.opts.DB.Compression.Encryption = domain.BLOB_ENCRYPTION_AES
	b.opts.DB.Compression.EncryptionKey = key
	return b
}

func (b *ArchiverBuilder) WithB2Encryption(encryption domain.BlobEncryption, key []byte) *ArchiverBuilder {
	b.opts.B2.Compression.Encryption = encryption
	b.opts.B2.Compression.EncryptionKey = key
	return b
}

func (b *ArchiverBuilder) WithDBEncryption(encryption domain.BlobEncryption, key []byte) *ArchiverBuilder {
	b.opts.DB.Compression.Encryption = encryption
	b.opts.DB.Compression.EncryptionKey = key
	return b
}

func (b *ArchiverBuilder) WithB2Credentials(credentials backblaze.Credentials) *ArchiverBuilder {
	b.opts.B2.Credentials = credentials
	return b
}

func (b *ArchiverBuilder) WithB2Salt(salt string) *ArchiverBuilder {
	b.opts.B2.Salt = salt
	return b
}

func (b *ArchiverBuilder) Build() (*service.ArchiverService, error) {
	if b.opts.DB.DB == nil {
		gLogger := utils.NewGLogger(b.opts.Logger, true).LogMode(logger.Warn)
		connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", b.dbUser, b.dbPassword, b.dbHost, b.dbPort, b.dbName)
		db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			return nil, err
		}
		b.opts.DB.DB = db
	}
	res, err := service.NewArchiverService(b.opts)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *ArchiverBuilder) ShouldBuild() *service.ArchiverService {
	res, err := b.Build()
	if err != nil {
		panic(err)
	}
	return res
}
