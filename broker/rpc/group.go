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

type groupRPCService struct {}

func NewGroupRPCService() *groupRPCService {
	return &groupRPCService{}
}

func (s *groupRPCService) JoinGroup(_ context.Context, request *paustqproto.JoinGroupRequest) (*paustqproto.JoinGroupResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) LeaveGroup(_ context.Context, request *paustqproto.LeaveGroupRequest) (*paustqproto.LeaveGroupResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) CreateGroup(_ context.Context, request *paustqproto.CreateGroupRequest) (*paustqproto.CreateGroupResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) ListGroups(_ context.Context, request *paustqproto.ListGroupsRequest) (*paustqproto.ListGroupsResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) DeleteGroup(_ context.Context, request *paustqproto.DeleteGroupRequest) (*paustqproto.DeleteGroupResponse, error) {
	return nil, errors.New("not implemented")
}


