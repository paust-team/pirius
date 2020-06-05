package rpc

import (
	"context"
	paustqproto "github.com/paust-team/paustq/proto"
)

type GroupRPCService interface {
	JoinGroup(context.Context, *paustqproto.JoinGroupRequest) *paustqproto.JoinGroupResponse
	LeaveGroup(context.Context, *paustqproto.LeaveGroupRequest) *paustqproto.LeaveGroupResponse
	CreateGroup(context.Context, *paustqproto.CreateGroupRequest) *paustqproto.CreateGroupResponse
	ListGroups(context.Context, *paustqproto.ListGroupsRequest) *paustqproto.ListGroupsResponse
	DeleteGroup(context.Context, *paustqproto.DeleteGroupRequest) *paustqproto.DeleteGroupResponse
}

type groupRPCService struct{}

func NewGroupRPCService() *groupRPCService {
	return &groupRPCService{}
}

func (s *groupRPCService) JoinGroup(_ context.Context, request *paustqproto.JoinGroupRequest) *paustqproto.JoinGroupResponse {
	return &paustqproto.JoinGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) LeaveGroup(_ context.Context, request *paustqproto.LeaveGroupRequest) *paustqproto.LeaveGroupResponse {
	return &paustqproto.LeaveGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) CreateGroup(_ context.Context, request *paustqproto.CreateGroupRequest) *paustqproto.CreateGroupResponse {
	return &paustqproto.CreateGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) ListGroups(_ context.Context, request *paustqproto.ListGroupsRequest) *paustqproto.ListGroupsResponse {
	return &paustqproto.ListGroupsResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}

func (s *groupRPCService) DeleteGroup(_ context.Context, request *paustqproto.DeleteGroupRequest) *paustqproto.DeleteGroupResponse {
	return &paustqproto.DeleteGroupResponse{ErrorCode: 1, ErrorMessage: "not implemented"}
}
