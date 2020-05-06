package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/zookeeper"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (service *APIServiceServer) Heartbeat(ctx context.Context, request *paustqproto.Ping) (*paustqproto.Pong, error) {

	serverTime := time.Now().Nanosecond()

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime)), nil
}

func (service *APIServiceServer) ShutdownBroker(ctx context.Context, request *paustqproto.ShutdownBrokerRequest) (*paustqproto.ShutdownBrokerResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return message.NewShutdownBrokerResponseMsg(), nil
}
