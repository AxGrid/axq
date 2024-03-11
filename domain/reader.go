/*
 * Created by Zed 06.12.2023, 11:12
 */

package domain

import "errors"

var (
	ErrB2FileNotFound = errors.New("b2 file not found")
)

type Reader interface {
	Pop() Message
	C() <-chan Message
	GetOpts() ServiceOpts
	Counter() (uint64, error)
	LastFID() (uint64, error)
	LastID() (uint64, error)
	Performance() uint64
}

type Message interface {
	Id() uint64
	Message() []byte
	Done()
	Error(err error)
}
