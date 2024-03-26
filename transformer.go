package axq

import (
	"github.com/axgrid/axq/domain"
)

type Transformer[T any] interface {
	Transform(from domain.Message) (T, error)
}
