package protobuf

/*
 __    _           ___
|  |  |_|_____ ___|_  |
|  |__| |     | .'|  _|
|_____|_|_|_|_|__,|___|
zed (06.03.2024)
*/

//go:generate protoc --go_out=./ --go_opt=Maxq.proto=../protobuf/ --proto_path=./ ./axq.proto
//protoc  --go_out=./  --proto_path=./protobufs/phttp/ --go_opt=Mphttp.proto=./target/generated-sources/proto/phttp/  ./protobufs/phttp/phttp.proto
