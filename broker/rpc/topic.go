package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TopicRPCService interface {
	CreateTopic(context.Context, *paustqproto.CreateTopicRequest) (*paustqproto.CreateTopicResponse, error)
	DeleteTopic(context.Context, *paustqproto.DeleteTopicRequest) (*paustqproto.DeleteTopicResponse, error)
}

type topicRPCService struct {
	DB       *storage.QRocksDB
	zkClient *zookeeper.ZKClient
}

func NewTopicRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *topicRPCService {
	return &topicRPCService{db, zkClient}
}

func (s topicRPCService) CreateTopic(ctx context.Context, request *paustqproto.CreateTopicRequest) (*paustqproto.CreateTopicResponse, error) {
	if err := s.DB.PutTopicIfNotExists(request.Topic.TopicName, request.Topic.TopicMeta,
		request.Topic.NumPartitions, request.Topic.ReplicationFactor); err != nil {
		return nil, err
	}

	err := s.zkClient.AddTopic(request.Topic.TopicName)
	if err != nil {
		if err == zk.ErrNodeExists {
			return nil, status.Error(codes.AlreadyExists, "topic already exists")
		}
		return nil, err
	}

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return message.NewCreateTopicResponseMsg(), nil
}

func (s topicRPCService) DeleteTopic(ctx context.Context, request *paustqproto.DeleteTopicRequest) (*paustqproto.DeleteTopicResponse, error) {

	if err := s.DB.DeleteTopic(request.TopicName); err != nil {
		return nil, err
	}

	if err := s.zkClient.RemoveTopic(request.TopicName); err != nil {
		return nil, err
	}

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "client canceled the request")
	}
	return message.NewDeleteTopicResponseMsg(), nil
}
