/*
 * Created by Zed 06.12.2023, 11:12
 */

package domain

import (
	"errors"
	"github.com/golang/protobuf/proto"
)

var (
	ErrB2FileNotFound = errors.New("b2 file not found")
)

type Message interface {
	Id() uint64
	Message() []byte
	Done()
	Error(err error)
	UnmarshalProto(v proto.Message) error
}
