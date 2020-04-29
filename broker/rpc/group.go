package rpc

import (
	"context"
	"errors"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GroupRPCService interface {
	JoinGroup(context.Context, *paustqproto.JoinGroupRequest) (*paustqproto.JoinGroupResponse, error)
	LeaveGroup(context.Context, *paustqproto.LeaveGroupRequest) (*paustqproto.LeaveGroupResponse, error)
	CreateGroup(context.Context, *paustqproto.CreateGroupRequest) (*paustqproto.CreateGroupResponse, error)
	ListGroups(context.Context, *paustqproto.ListGroupsRequest) (*paustqproto.ListGroupsResponse, error)
	DeleteGroup(context.Context, *paustqproto.DeleteGroupRequest) (*paustqproto.DeleteGroupResponse, error)
}

type groupRPCService struct{}

func NewGroupRPCService() *groupRPCService {
	return &groupRPCService{}
}

func (s *groupRPCService) JoinGroup(ctx context.Context, request *paustqproto.JoinGroupRequest) (*paustqproto.JoinGroupResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) LeaveGroup(ctx context.Context, request *paustqproto.LeaveGroupRequest) (*paustqproto.LeaveGroupResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) CreateGroup(ctx context.Context, request *paustqproto.CreateGroupRequest) (*paustqproto.CreateGroupResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) ListGroups(ctx context.Context, request *paustqproto.ListGroupsRequest) (*paustqproto.ListGroupsResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}

func (s *groupRPCService) DeleteGroup(ctx context.Context, request *paustqproto.DeleteGroupRequest) (*paustqproto.DeleteGroupResponse, error) {
	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return nil, errors.New("not implemented")
}
