package domain

type MessageIDs struct {
	FID uint64
	Id  uint64
}

type Counter interface {
	Get() (uint64, error)
	Set(id uint64)
	LastId() uint64
}
