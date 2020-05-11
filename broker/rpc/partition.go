package rpc

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/storage"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PartitionRPCService interface {
	CreatePartition(context.Context, *paustqproto.CreatePartitionRequest) (*paustqproto.CreatePartitionResponse, error)
}

type partitionRPCService struct {
	DB       *storage.QRocksDB
	zkClient *zookeeper.ZKClient
}

func NewPartitionRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *partitionRPCService {
	return &partitionRPCService{db, zkClient}
}

func (s *partitionRPCService) CreatePartition(ctx context.Context, request *paustqproto.CreatePartitionRequest) (*paustqproto.CreatePartitionResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}
