package domain

type Service interface {
	GetOpts() ServiceOpts
	GetPerformance() uint64
}
