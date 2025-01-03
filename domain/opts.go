/*
 * Created by Zed 05.12.2023, 21:24
 */

package domain

import (
	"context"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"gopkg.in/kothar/go-backblaze.v0"
	"gorm.io/gorm"
	"time"
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
	PartitionsCount int
	MaxBlobSize     int
	CutSize         int
	CutFrequency    time.Duration
	UUID            uuid.UUID
}

func (w *WriterOptions) GetType() string {
	return "Writer"
}

func (w *WriterOptions) GetName() string {
	return w.BaseOptions.Name
}

func (w *WriterOptions) GetReaderName() string {
	return "-"
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

func (r *ReaderOptions) GetType() string {
	return "Reader"
}

func (r *ReaderOptions) GetName() string {
	return r.BaseOptions.Name
}

func (r *ReaderOptions) GetReaderName() string {
	return r.ReaderName
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
	DB             DataBaseOptions
	B2             B2Options
	Reader         ReaderOptions
	ChunkSize      int
	MaxSize        int
	MaxCount       int
	OuterCount     int
	CleanTimeout   int
	Intersection   uint64
	DeprecatedFrom uint64
}

type B2Options struct {
	backblaze.Credentials
	Salt        string
	Endpoint    string
	Compression CompressionOptions
}

type ReaderTransformerOptions[T any] struct {
	ReaderOptions
	Middlewares []ReaderTransformMiddlewareFunc[T]
}

type WriterTransformerOptions[F any] struct {
	WriterOptions
	Middlewares []WriterTransformMiddlewareFunc[F]
}

type ServiceOpts interface {
	GetType() string
	GetName() string
	GetReaderName() string
}
