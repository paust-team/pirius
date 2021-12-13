package rpc

import (
	"github.com/paust-team/shapleq/message"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/zookeeper"
	"time"
)

type ConnectionRPCService interface {
	Heartbeat(*shapleqproto.Ping) *shapleqproto.Pong
}

type connectionRPCService struct {
	zkClient *zookeeper.ZKClient
}

func NewConnectionRPCService(zkClient *zookeeper.ZKClient) *connectionRPCService {
	return &connectionRPCService{zkClient}
}

func (s *connectionRPCService) Heartbeat(request *shapleqproto.Ping) *shapleqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}
