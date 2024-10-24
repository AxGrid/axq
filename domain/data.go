/*
 * Created by Zed 05.12.2023, 21:13
 */

package domain

type Blob struct {
	FID         uint64 `gorm:"primaryKey;column:fid"`
	Compression BlobCompression
	Encryption  BlobEncryption
	Total       int
	FromId      uint64
	ToId        uint64
	Message     []byte
}

type BlobCounter struct {
	ReaderName string `gorm:"size:128;primaryKey"`
	Name       string `gorm:"size:128;primaryKey"`
	ID         uint64
}

func (b *BlobCounter) TableName() string {
	return "axq_counters"
}
