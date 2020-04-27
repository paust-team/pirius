package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"time"
)

type APIServiceServer struct {
	TopicRPCService
	PartitionRPCService
	ConfigRPCService
	GroupRPCService
}

func NewAPIServiceServer(db *storage.QRocksDB) *APIServiceServer {
	return &APIServiceServer{
		NewTopicRPCService(db),
		NewPartitionRPCService(db),
		NewConfigRPCService(),
		NewGroupRPCService(),
	}
}

func (service *APIServiceServer) Heartbeat(_ context.Context, request *paustqproto.Ping) (*paustqproto.Pong, error) {

	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime)), nil
}

func (service *APIServiceServer) ShutdownBroker(_ context.Context, request *paustqproto.ShutdownBrokerRequest) (*paustqproto.ShutdownBrokerResponse, error) {
	return message.NewShutdownBrokerResponseMsg(), nil
}
