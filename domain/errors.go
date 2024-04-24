package domain

import "errors"

var (
	ErrB2FileNotFound = errors.New("b2 file not found")
	ErrSkipMessage    = errors.New("")
	ErrB2AndDBGap     = errors.New("gap between b2 and db data")
)
