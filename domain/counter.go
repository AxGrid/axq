package domain

type BlobIDs struct {
	FID    uint64
	FromId uint64
	ToId   uint64
	// DbFid — fid блоба БД, из которого пришло последнее сообщение этого
	// архива. Всё, что в базе лежит строго ниже него, заархивировано целиком.
	DbFid uint64
}

type MessageIDs struct {
	// FID — позиция в таблице очереди
	FID uint64
	// B2Fid — номер файла в архиве; заполняет только архивер
	B2Fid uint64
	Id    uint64
}

type Counter interface {
	Get() (uint64, error)
	Set(id uint64)
	LastId() uint64
}
