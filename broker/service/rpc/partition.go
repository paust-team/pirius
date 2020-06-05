package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/storage"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
)

type PartitionRPCService interface {
	CreatePartition(context.Context, *paustqproto.CreatePartitionRequest) *paustqproto.CreatePartitionResponse
}

type partitionRPCService struct {
	DB       *storage.QRocksDB
	zkClient *zookeeper.ZKClient
}

func NewPartitionRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *partitionRPCService {
	return &partitionRPCService{db, zkClient}
}

func (s *partitionRPCService) CreatePartition(_ context.Context, request *paustqproto.CreatePartitionRequest) *paustqproto.CreatePartitionResponse {

	return &paustqproto.CreatePartitionResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
