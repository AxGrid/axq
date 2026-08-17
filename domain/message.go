package domain

import "github.com/golang/protobuf/proto"

type Message interface {
	Id() uint64
	// Fid — номер блоба, из которого приехало сообщение, в том хранилище,
	// которое читает этот ридер: для ридера очереди это fid строки в таблице
	// БД, для b2-ридера — номер файла в B2. Нужен потребителям, которым важно
	// не само сообщение, а докуда целиком дочитан источник.
	Fid() uint64
	Message() []byte
	Done()
	Error(err error)
	UnmarshalProto(v proto.Message) error
}
