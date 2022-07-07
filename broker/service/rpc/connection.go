package rpc

import (
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/message"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"time"
)

type ConnectionRPCService interface {
	Heartbeat(*shapleqproto.Ping) *shapleqproto.Pong
}

type connectionRPCService struct {
	coordiWrapper *coordinator_helper.CoordinatorWrapper
}

func NewConnectionRPCService(coordiWrapper *coordinator_helper.CoordinatorWrapper) *connectionRPCService {
	return &connectionRPCService{coordiWrapper}
}

func (s *connectionRPCService) Heartbeat(request *shapleqproto.Ping) *shapleqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}
