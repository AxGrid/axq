/*
 * Created by Zed 08.12.2023, 14:43
 */

package domain

import "github.com/golang/protobuf/proto"

type Writer interface {
	Push(message []byte) error
	PushMany(messages [][]byte) error
	PushProto(message proto.Message) error
	PushProtoMany(messages []proto.Message) error
	Close()
	GetOpts() ServiceOpts
	Counter() (uint64, error)
	LastFID() (uint64, error)
	LastID() (uint64, error)
	Performance() uint64
}
