package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"time"
)

type APIServiceServer struct {
	TopicRPCService
	PartitionRPCService
	ConfigRPCService
	GroupRPCService
}

func NewAPIServiceServer(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *APIServiceServer {

	return &APIServiceServer{
		NewTopicRPCService(db, zkClient),
		NewPartitionRPCService(db, zkClient),
		NewConfigRPCService(),
		NewGroupRPCService(),
	}
}

func (service *APIServiceServer) Heartbeat(_ context.Context, request *paustqproto.Ping) *paustqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}

func (service *APIServiceServer) ShutdownBroker(_ context.Context, request *paustqproto.ShutdownBrokerRequest) *paustqproto.ShutdownBrokerResponse {
	return &paustqproto.ShutdownBrokerResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
