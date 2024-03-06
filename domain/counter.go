package domain

type Counter interface {
	Get() (uint64, error)
	Set(id uint64)
	LastId() uint64
}
