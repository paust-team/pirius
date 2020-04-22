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

type configRPCService struct {}

func NewConfigRPCService() *configRPCService {
	return &configRPCService{}
}

func (s *configRPCService) SetConfig(_ context.Context, request *paustqproto.SetConfigRequest) (*paustqproto.SetConfigResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *configRPCService) AlterConfig(_ context.Context, request *paustqproto.AlterConfigRequest) (*paustqproto.AlterConfigResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *configRPCService) ShowConfig(_ context.Context, request *paustqproto.ShowConfigRequest) (*paustqproto.ShowConfigResponse, error) {
	return nil, errors.New("not implemented")
}


