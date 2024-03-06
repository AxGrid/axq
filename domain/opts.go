/*
 * Created by Zed 05.12.2023, 21:24
 */

package domain

import (
	"context"
	"github.com/rs/zerolog"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
)

const (
	B2TokenTTL = 23
)

type BlobCompression int32
type BlobEncryption int32

const (
	BLOB_COMPRESSION_NONE = 0
	BLOB_COMPRESSION_GZIP = 1
)

const (
	BLOB_ENCRYPTION_NONE = 0
	BLOB_ENCRYPTION_AES  = 1
)

func (b BlobCompression) String() string {
	return [...]string{"none", "gzip"}[b]
}

func (b BlobEncryption) String() string {
	return [...]string{"none", "aes"}[b]
}

type BaseOptions struct {
	CTX    context.Context
	Logger zerolog.Logger
	Name   string
}

type DataBaseOptions struct {
	DB          *gorm.DB
	Compression CompressionOptions
}

type CompressionOptions struct {
	Compression   BlobCompression
	Encryption    BlobEncryption
	EncryptionKey []byte
}

type WriterOptions struct {
	BaseOptions
	DB              DataBaseOptions
	CreateQueueSize int
	ChunkSize       int
	MaxBlobSize     int
}

func (w *WriterOptions) GetType() string {
	return "Writer"
}

func (w *WriterOptions) GetName() string {
	return w.BaseOptions.Name
}

type ReaderOptions struct {
	BaseOptions
	DB          DataBaseOptions
	B2          B2Options
	B2Bucket    *backblaze.Bucket
	ReaderName  string
	BufferSize  int
	BatchSize   uint64
	LoaderCount int
	WaiterCount int
	LastId      *LastIdOptions
}

type B2ReaderOptions struct {
	BaseOptions
	B2          B2Options
	DB          DataBaseOptions
	ReaderName  string
	LoaderCount int
	OuterCount  int
	BufferSize  int
	LastId      *LastIdOptions
}

type LastIdOptions struct {
	FID    uint64
	LastId uint64
}

type ArchiverOptions struct {
	BaseOptions
	DB        DataBaseOptions
	B2        B2Options
	ChunkSize int
	MaxSize   int
	MaxCount  int
}

type B2Options struct {
	backblaze.Credentials
	Salt        string
	Endpoint    string
	Compression CompressionOptions
}

type ServiceOpts interface {
	GetType() string
	GetName() string
}
