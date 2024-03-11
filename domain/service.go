package domain

type Service interface {
	GetOpts() ServiceOpts
	Counter() (uint64, error)
	LastFID() (uint64, error)
	LastID() (uint64, error)
	Performance() uint64
}

type Group []Service
