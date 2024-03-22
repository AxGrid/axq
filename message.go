package axq

import "github.com/golang/protobuf/proto"

type Message interface {
	Id() uint64
	Message() []byte
	Done()
	Error(err error)
	UnmarshalProto(v proto.Message) error
}

type ReaderMessageHolder[T any] interface {
	Msg() T
	Done()
	Error(err error)
}
