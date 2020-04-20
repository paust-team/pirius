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

type PartitionRPCServiceServer struct {
	DB  	*storage.QRocksDB
}

func NewPartitionRPCServiceServer(db *storage.QRocksDB) *PartitionRPCServiceServer{
	return &PartitionRPCServiceServer{db}
}

func (s *PartitionRPCServiceServer) CreatePartition(_ context.Context, request *paustqproto.CreatePartitionRequest) (*paustqproto.CreatePartitionResponse, error) {
	return nil, errors.New("not implemented")
}