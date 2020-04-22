package rpc

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
)

type TopicRPCService interface {
	CreateTopic(context.Context, *paustqproto.CreateTopicRequest) (*paustqproto.CreateTopicResponse, error)
	DeleteTopic(context.Context, *paustqproto.DeleteTopicRequest) (*paustqproto.DeleteTopicResponse, error)
	DescribeTopic(context.Context, *paustqproto.DescribeTopicRequest) (*paustqproto.DescribeTopicResponse, error)
	ListTopics(context.Context, *paustqproto.ListTopicsRequest) (*paustqproto.ListTopicsResponse, error)
}

type TopicRPCServiceServer struct {
	DB  	*storage.QRocksDB
}

func NewTopicRPCServiceServer(db *storage.QRocksDB) *TopicRPCServiceServer{
	return &TopicRPCServiceServer{db}
}

func (s TopicRPCServiceServer) CreateTopic(_ context.Context, request *paustqproto.CreateTopicRequest) (*paustqproto.CreateTopicResponse, error) {

	if err := s.DB.PutTopicIfNotExists(request.Topic.TopicName, request.Topic.TopicMeta,
		request.Topic.NumPartitions, request.Topic.ReplicationFactor); err != nil {
		return nil, err
	}

	return message.NewCreateTopicResponseMsg(), nil
}

func (s TopicRPCServiceServer) DeleteTopic(_ context.Context, request *paustqproto.DeleteTopicRequest) (*paustqproto.DeleteTopicResponse, error) {

	if err := s.DB.DeleteTopic(request.TopicName); err != nil {
		return nil, err
	}
	return message.NewDeleteTopicResponseMsg(), nil
}

func (s TopicRPCServiceServer) DescribeTopic(_ context.Context, request *paustqproto.DescribeTopicRequest) (*paustqproto.DescribeTopicResponse, error) {

	result, err := s.DB.GetTopic(request.TopicName)

	if err != nil {
		return nil, err
	}

	if result == nil || !result.Exists() {
		return nil, errors.New("topic not exists")
	}
	topicValue := storage.NewTopicValue(result)
	topic := message.NewTopicMsg(request.TopicName, topicValue.TopicMeta(), topicValue.NumPartitions(), topicValue.ReplicationFactor())

	return message.NewDescribeTopicResponseMsg(topic, 1,1), nil
}

func (s TopicRPCServiceServer) ListTopics(_ context.Context, _ *paustqproto.ListTopicsRequest) (*paustqproto.ListTopicsResponse, error) {

	var topics []*paustqproto.Topic
	topicMap := s.DB.GetAllTopics()
	for topicName, topicValue := range topicMap {
		topic := message.NewTopicMsg(topicName, topicValue.TopicMeta(),topicValue.NumPartitions(),topicValue.ReplicationFactor())
		topics = append(topics, topic)
	}

	return message.NewListTopicsResponseMsg(topics), nil
}