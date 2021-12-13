package rpc

import (
	"github.com/paust-team/shapleq/message"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
	"time"
)

type ConnectionRPCService interface {
	Heartbeat(*shapleqproto.Ping) *shapleqproto.Pong
}

type connectionRPCService struct {
	zkqClient *zookeeper.ZKQClient
}

func NewConnectionRPCService(zkqClient *zookeeper.ZKQClient) *connectionRPCService {
	return &connectionRPCService{zkqClient}
}

func (s *connectionRPCService) Heartbeat(request *shapleqproto.Ping) *shapleqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}
