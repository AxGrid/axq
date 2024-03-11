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
	GetPerformance() uint64
}
