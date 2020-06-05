package rpc

import (
	"context"
	paustqproto "github.com/paust-team/paustq/proto"
)

type ConfigRPCService interface {
	SetConfig(context.Context, *paustqproto.SetConfigRequest) *paustqproto.SetConfigResponse
	AlterConfig(context.Context, *paustqproto.AlterConfigRequest) *paustqproto.AlterConfigResponse
	ShowConfig(context.Context, *paustqproto.ShowConfigRequest) *paustqproto.ShowConfigResponse
}

type configRPCService struct{}

func NewConfigRPCService() *configRPCService {
	return &configRPCService{}
}

func (s *configRPCService) SetConfig(_ context.Context, request *paustqproto.SetConfigRequest) *paustqproto.SetConfigResponse {
	return &paustqproto.SetConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *configRPCService) AlterConfig(_ context.Context, request *paustqproto.AlterConfigRequest) *paustqproto.AlterConfigResponse {
	return &paustqproto.AlterConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *configRPCService) ShowConfig(_ context.Context, request *paustqproto.ShowConfigRequest) *paustqproto.ShowConfigResponse {
	return &paustqproto.ShowConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
