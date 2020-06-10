package rpc

import (
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"time"
)

type HeartBeatRPCService interface {
	Heartbeat(ping *paustqproto.Ping) *paustqproto.Pong
}

type heartBeatRPCService struct {}

func NewHeartbeatService() *heartBeatRPCService {
	return &heartBeatRPCService{}
}

func (s *heartBeatRPCService) Heartbeat(request *paustqproto.Ping) *paustqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}