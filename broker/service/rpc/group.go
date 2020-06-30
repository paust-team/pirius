package rpc

import (
	paustqproto "github.com/paust-team/paustq/proto"
)

type GroupRPCService interface {
	JoinGroup(*paustqproto.JoinGroupRequest) *paustqproto.JoinGroupResponse
	LeaveGroup(*paustqproto.LeaveGroupRequest) *paustqproto.LeaveGroupResponse
	CreateGroup(*paustqproto.CreateGroupRequest) *paustqproto.CreateGroupResponse
	ListGroups(*paustqproto.ListGroupsRequest) *paustqproto.ListGroupsResponse
	DeleteGroup(*paustqproto.DeleteGroupRequest) *paustqproto.DeleteGroupResponse
}

type groupRPCService struct{}

func NewGroupRPCService() *groupRPCService {
	return &groupRPCService{}
}

func (s *groupRPCService) JoinGroup(request *paustqproto.JoinGroupRequest) *paustqproto.JoinGroupResponse {
	return &paustqproto.JoinGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) LeaveGroup(request *paustqproto.LeaveGroupRequest) *paustqproto.LeaveGroupResponse {
	return &paustqproto.LeaveGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) CreateGroup(request *paustqproto.CreateGroupRequest) *paustqproto.CreateGroupResponse {
	return &paustqproto.CreateGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) ListGroups(request *paustqproto.ListGroupsRequest) *paustqproto.ListGroupsResponse {
	return &paustqproto.ListGroupsResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) DeleteGroup(request *paustqproto.DeleteGroupRequest) *paustqproto.DeleteGroupResponse {
	return &paustqproto.DeleteGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
