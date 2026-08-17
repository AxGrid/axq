/*
 * Created by Zed 05.12.2023, 21:24
 */

package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
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
	PartitionsCount int
	MaxBlobSize     int
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
	DB                    DataBaseOptions
	ReaderName            string
	BufferSize            int
	BatchSize             uint64
	LoaderCount           int
	WaiterCount           int
	LastId                *LastIdOptions
	StartFromEnd          bool
	FromLatest            bool
	StartFromEndEveryTime bool
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
	DB         DataBaseOptions
	B2         B2Options
	Reader ReaderOptions
	// Prefix — устаревшее имя стенда. Живёт ради старых сборок: если B2.Stand
	// пуст, стенд берётся отсюда.
	Prefix     string
	ChunkSize  int
	MaxSize    int
	MaxCount   int
	OuterCount int

	// CleanGapFID — зазор в блобах, который чистка оставляет позади самого
	// отставшего потребителя очереди. Удаляется всё ниже (минимальный fid
	// счётчиков − CleanGapFID), но не выше заархивированного в B2.
	// Ноль выключает чистку.
	CleanGapFID uint64
	// CleanInterval — как часто проверять, есть ли что удалять.
	CleanInterval time.Duration
	// CleanBatch — сколько строк удалять одним DELETE, чтобы не держать
	// длинную транзакцию на большой таблице.
	CleanBatch int
}

// CleanStats — снимок состояния чистки на последнем проходе. Нужен, чтобы
// вешать алерты на отставание потребителей: чистка сама по себе молча стоит,
// если кто-то встал.
type CleanStats struct {
	// SlowestReader — имя самого отставшего потребителя очереди
	SlowestReader string
	// SlowestReaderFID — его позиция в блобах таблицы
	SlowestReaderFID uint64
	// HeadFID — последний записанный блоб
	HeadFID uint64
	// ArchivedFID — докуда таблица целиком уехала в B2
	ArchivedFID uint64
	// LastDeletedFID — граница последнего удаления
	LastDeletedFID uint64
	// DeletedRows — сколько строк удалено с момента запуска
	DeletedRows int64
	// LastRun — когда последний раз отрабатывал проход
	LastRun time.Time
	// NoReaders — очередь никто не читает, чистка не рискует удалять
	NoReaders bool
}

// ReaderLag — на сколько блобов самый медленный потребитель отстал от головы.
func (s CleanStats) ReaderLag() uint64 {
	if s.HeadFID < s.SlowestReaderFID {
		return 0
	}
	return s.HeadFID - s.SlowestReaderFID
}

type B2Options struct {
	backblaze.Credentials
	Salt     string
	Endpoint string
	// Bucket — полное имя бакета. Задаётся, когда данные уже лежат в бакете со
	// старым именем: переименовать бакет в B2 нельзя.
	Bucket string
	// Namespace — первая часть собираемого имени, общая для всех очередей
	// проекта. Пусто означает axq.
	Namespace string
	// Stand — окружение во второй части имени: prod, stage, dev.
	Stand       string
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
