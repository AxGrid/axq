package domain

import (
	"github.com/google/uuid"
	"time"
)

type Allow struct {
	UUID       uuid.UUID
	WriterName string
	CreatedAt  time.Time
}
