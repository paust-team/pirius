package rpc

import (
	"errors"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
)

type TopicRPCService interface {
	CreateTopic(*paustqproto.CreateTopicRequest) *paustqproto.CreateTopicResponse
	DeleteTopic(*paustqproto.DeleteTopicRequest) *paustqproto.DeleteTopicResponse
	ListTopic(*paustqproto.ListTopicRequest) *paustqproto.ListTopicResponse
	DescribeTopic(*paustqproto.DescribeTopicRequest) *paustqproto.DescribeTopicResponse
}

type topicRPCService struct {
	DB       *storage.QRocksDB
	zkClient *zookeeper.ZKClient
}

func NewTopicRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *topicRPCService {
	return &topicRPCService{db, zkClient}
}

func (s topicRPCService) CreateTopic(request *paustqproto.CreateTopicRequest) *paustqproto.CreateTopicResponse {

	topicValue := internals.NewTopicMetaFromValues(request.Topic.Description, request.Topic.NumPartitions, request.Topic.ReplicationFactor)
	err := s.zkClient.AddTopic(request.Topic.Name, topicValue)
	if err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if errors.As(err, &e) {
			return message.NewCreateTopicResponseMsg(e)
		}
		return message.NewCreateTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewCreateTopicResponseMsg(nil)
}

func (s topicRPCService) DeleteTopic(request *paustqproto.DeleteTopicRequest) *paustqproto.DeleteTopicResponse {

	if err := s.zkClient.RemoveTopic(request.TopicName); err != nil {
		return message.NewDeleteTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDeleteTopicResponseMsg(nil)
}

func (s topicRPCService) ListTopic(_ *paustqproto.ListTopicRequest) *paustqproto.ListTopicResponse {

	topics, err := s.zkClient.GetTopics()
	if err != nil {
		return message.NewListTopicResponseMsg(nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewListTopicResponseMsg(topics, nil)
}

func (s topicRPCService) DescribeTopic(request *paustqproto.DescribeTopicRequest) *paustqproto.DescribeTopicResponse {

	topicValue, err := s.zkClient.GetTopic(request.TopicName)
	if err != nil {
		return message.NewDescribeTopicResponseMsg("", "", 0, 0, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDescribeTopicResponseMsg(request.TopicName, topicValue.TopicMeta(), topicValue.NumPartitions(), topicValue.ReplicationFactor(), nil)
}