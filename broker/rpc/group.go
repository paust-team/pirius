package rpc

import (
	"context"
	"errors"
	paustqproto "github.com/paust-team/paustq/proto"
)

type GroupRPCService interface {
	JoinGroup(context.Context, *paustqproto.JoinGroupRequest) (*paustqproto.JoinGroupResponse, error)
	LeaveGroup(context.Context, *paustqproto.LeaveGroupRequest) (*paustqproto.LeaveGroupResponse, error)
	CreateGroup(context.Context, *paustqproto.CreateGroupRequest) (*paustqproto.CreateGroupResponse, error)
	ListGroups(context.Context, *paustqproto.ListGroupsRequest) (*paustqproto.ListGroupsResponse, error)
	DeleteGroup(context.Context, *paustqproto.DeleteGroupRequest) (*paustqproto.DeleteGroupResponse, error)
}

type GroupRPCServiceServer struct {}

func NewGroupRPCServiceServer() *GroupRPCServiceServer {
	return &GroupRPCServiceServer{}
}

func (s *GroupRPCServiceServer) JoinGroup(_ context.Context, request *paustqproto.JoinGroupRequest) (*paustqproto.JoinGroupResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *GroupRPCServiceServer) LeaveGroup(_ context.Context, request *paustqproto.LeaveGroupRequest) (*paustqproto.LeaveGroupResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *GroupRPCServiceServer) CreateGroup(_ context.Context, request *paustqproto.CreateGroupRequest) (*paustqproto.CreateGroupResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *GroupRPCServiceServer) ListGroups(_ context.Context, request *paustqproto.ListGroupsRequest) (*paustqproto.ListGroupsResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *GroupRPCServiceServer) DeleteGroup(_ context.Context, request *paustqproto.DeleteGroupRequest) (*paustqproto.DeleteGroupResponse, error) {
	return nil, errors.New("not implemented")
}


