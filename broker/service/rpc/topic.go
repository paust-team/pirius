package rpc

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
)

type TopicRPCService interface {
	CreateTopic(context.Context, *paustqproto.CreateTopicRequest) *paustqproto.CreateTopicResponse
	DeleteTopic(context.Context, *paustqproto.DeleteTopicRequest) *paustqproto.DeleteTopicResponse
}

type topicRPCService struct {
	DB       *storage.QRocksDB
	zkClient *zookeeper.ZKClient
}

func NewTopicRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *topicRPCService {
	return &topicRPCService{db, zkClient}
}

func (s topicRPCService) CreateTopic(_ context.Context, request *paustqproto.CreateTopicRequest) *paustqproto.CreateTopicResponse {

	if err := s.DB.PutTopicIfNotExists(request.Topic.TopicName, request.Topic.TopicMeta,
		request.Topic.NumPartitions, request.Topic.ReplicationFactor); err != nil {
		return message.NewCreateTopicResponseMsg(&pqerror.QRocksOperateError{ErrStr: err.Error()})
	}

	err := s.zkClient.AddTopic(request.Topic.TopicName)
	if err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if errors.As(err, &e) {
			return message.NewCreateTopicResponseMsg(e)
		}
		return message.NewCreateTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewCreateTopicResponseMsg(nil)
}

func (s topicRPCService) DeleteTopic(_ context.Context, request *paustqproto.DeleteTopicRequest) *paustqproto.DeleteTopicResponse {

	if err := s.DB.DeleteTopic(request.TopicName); err != nil {
		return message.NewDeleteTopicResponseMsg(&pqerror.QRocksOperateError{ErrStr: err.Error()})
	}
	if err := s.zkClient.RemoveTopic(request.TopicName); err != nil {
		return message.NewDeleteTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDeleteTopicResponseMsg(nil)
}
