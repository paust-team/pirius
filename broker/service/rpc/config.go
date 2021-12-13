package rpc

import (
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
)

type ConfigRPCService interface {
	SetConfig(*shapleqproto.SetConfigRequest) *shapleqproto.SetConfigResponse
	AlterConfig(*shapleqproto.AlterConfigRequest) *shapleqproto.AlterConfigResponse
	ShowConfig(*shapleqproto.ShowConfigRequest) *shapleqproto.ShowConfigResponse
}

type configRPCService struct{}

func NewConfigRPCService() *configRPCService {
	return &configRPCService{}
}

func (s *configRPCService) SetConfig(request *shapleqproto.SetConfigRequest) *shapleqproto.SetConfigResponse {
	return &shapleqproto.SetConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *configRPCService) AlterConfig(request *shapleqproto.AlterConfigRequest) *shapleqproto.AlterConfigResponse {
	return &shapleqproto.AlterConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *configRPCService) ShowConfig(request *shapleqproto.ShowConfigRequest) *shapleqproto.ShowConfigResponse {
	return &shapleqproto.ShowConfigResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
