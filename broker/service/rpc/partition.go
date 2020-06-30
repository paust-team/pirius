package rpc

import (
	"github.com/paust-team/shapleq/broker/storage"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/zookeeper"
)

type PartitionRPCService interface {
	CreatePartition(*shapleqproto.CreatePartitionRequest) *shapleqproto.CreatePartitionResponse
}

type partitionRPCService struct {
	DB       *storage.QRocksDB
	zkClient *zookeeper.ZKClient
}

func NewPartitionRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *partitionRPCService {
	return &partitionRPCService{db, zkClient}
}

func (s *partitionRPCService) CreatePartition(request *shapleqproto.CreatePartitionRequest) *shapleqproto.CreatePartitionResponse {

	return &shapleqproto.CreatePartitionResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
