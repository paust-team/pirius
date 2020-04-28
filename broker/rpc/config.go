package rpc

import (
	"context"
	"errors"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ConfigRPCService interface {
	SetConfig(context.Context, *paustqproto.SetConfigRequest) (*paustqproto.SetConfigResponse, error)
	AlterConfig(context.Context, *paustqproto.AlterConfigRequest) (*paustqproto.AlterConfigResponse, error)
	ShowConfig(context.Context, *paustqproto.ShowConfigRequest) (*paustqproto.ShowConfigResponse, error)
}

type configRPCService struct{}

func NewConfigRPCService() *configRPCService {
	return &configRPCService{}
}

func (s *configRPCService) SetConfig(ctx context.Context, request *paustqproto.SetConfigRequest) (*paustqproto.SetConfigResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}

func (s *configRPCService) AlterConfig(ctx context.Context, request *paustqproto.AlterConfigRequest) (*paustqproto.AlterConfigResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}

func (s *configRPCService) ShowConfig(ctx context.Context, request *paustqproto.ShowConfigRequest) (*paustqproto.ShowConfigResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}
