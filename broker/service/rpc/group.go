package rpc

import (
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
)

type GroupRPCService interface {
	JoinGroup(*shapleqproto.JoinGroupRequest) *shapleqproto.JoinGroupResponse
	LeaveGroup(*shapleqproto.LeaveGroupRequest) *shapleqproto.LeaveGroupResponse
	CreateGroup(*shapleqproto.CreateGroupRequest) *shapleqproto.CreateGroupResponse
	ListGroups(*shapleqproto.ListGroupsRequest) *shapleqproto.ListGroupsResponse
	DeleteGroup(*shapleqproto.DeleteGroupRequest) *shapleqproto.DeleteGroupResponse
}

type groupRPCService struct{}

func NewGroupRPCService() *groupRPCService {
	return &groupRPCService{}
}

func (s *groupRPCService) JoinGroup(request *shapleqproto.JoinGroupRequest) *shapleqproto.JoinGroupResponse {
	return &shapleqproto.JoinGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) LeaveGroup(request *shapleqproto.LeaveGroupRequest) *shapleqproto.LeaveGroupResponse {
	return &shapleqproto.LeaveGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) CreateGroup(request *shapleqproto.CreateGroupRequest) *shapleqproto.CreateGroupResponse {
	return &shapleqproto.CreateGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) ListGroups(request *shapleqproto.ListGroupsRequest) *shapleqproto.ListGroupsResponse {
	return &shapleqproto.ListGroupsResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) DeleteGroup(request *shapleqproto.DeleteGroupRequest) *shapleqproto.DeleteGroupResponse {
	return &shapleqproto.DeleteGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
