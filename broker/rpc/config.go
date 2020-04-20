package rpc

import (
	"context"
	"errors"
	paustqproto "github.com/paust-team/paustq/proto"
)

type ConfigRPCService interface {
	SetConfig(context.Context, *paustqproto.SetConfigRequest) (*paustqproto.SetConfigResponse, error)
	AlterConfig(context.Context, *paustqproto.AlterConfigRequest) (*paustqproto.AlterConfigResponse, error)
	ShowConfig(context.Context, *paustqproto.ShowConfigRequest) (*paustqproto.ShowConfigResponse, error)
}

type ConfigRPCServiceServer struct {}

func NewConfigRPCServiceServer() *ConfigRPCServiceServer {
	return &ConfigRPCServiceServer{}
}

func (s *ConfigRPCServiceServer) SetConfig(_ context.Context, request *paustqproto.SetConfigRequest) (*paustqproto.SetConfigResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *ConfigRPCServiceServer) AlterConfig(_ context.Context, request *paustqproto.AlterConfigRequest) (*paustqproto.AlterConfigResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *ConfigRPCServiceServer) ShowConfig(_ context.Context, request *paustqproto.ShowConfigRequest) (*paustqproto.ShowConfigResponse, error) {
	return nil, errors.New("not implemented")
}


