package rpc

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/storage"
	paustqproto "github.com/paust-team/paustq/proto"
)

type PartitionRPCService interface {
	CreatePartition(context.Context, *paustqproto.CreatePartitionRequest) (*paustqproto.CreatePartitionResponse, error)
}

type partitionRPCService struct {
	DB *storage.QRocksDB
}

func NewPartitionRPCService(db *storage.QRocksDB) *partitionRPCService {
	return &partitionRPCService{db}
}

func (s *partitionRPCService) CreatePartition(_ context.Context, request *paustqproto.CreatePartitionRequest) (*paustqproto.CreatePartitionResponse, error) {
	return nil, errors.New("not implemented")
}
