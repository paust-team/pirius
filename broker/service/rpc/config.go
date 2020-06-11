package rpc

import (
	paustqproto "github.com/paust-team/paustq/proto"
)

type ConfigRPCService interface {
	SetConfig(*paustqproto.SetConfigRequest) *paustqproto.SetConfigResponse
	AlterConfig(*paustqproto.AlterConfigRequest) *paustqproto.AlterConfigResponse
	ShowConfig(*paustqproto.ShowConfigRequest) *paustqproto.ShowConfigResponse
}

type configRPCService struct{}

func NewConfigRPCService() *configRPCService {
	return &configRPCService{}
}

func (s *configRPCService) SetConfig(request *paustqproto.SetConfigRequest) *paustqproto.SetConfigResponse {
	return &paustqproto.SetConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *configRPCService) AlterConfig(request *paustqproto.AlterConfigRequest) *paustqproto.AlterConfigResponse {
	return &paustqproto.AlterConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *configRPCService) ShowConfig(request *paustqproto.ShowConfigRequest) *paustqproto.ShowConfigResponse {
	return &paustqproto.ShowConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
