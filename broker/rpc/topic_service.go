package rpc

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/storage"
	paustq_proto "github.com/paust-team/paustq/proto"
)

type TopicServiceServer struct {
	DB  	*storage.QRocksDB
}

func NewTopicServiceServer(db *storage.QRocksDB) *TopicServiceServer {
	return &TopicServiceServer{db}
}

func (s *TopicServiceServer) CreateTopic(_ context.Context, request *paustq_proto.CreateTopicRequest) (*paustq_proto.CreateTopicResponse, error) {

	result, err := s.DB.GetTopic(request.Topic.TopicName)

	if err != nil {
		return nil, err
	}

	if result != nil && result.Exists() {
		return nil, errors.New("topic already exists")
	}

	if err := s.DB.PutTopic(request.Topic.TopicName, request.Topic.TopicMeta,
		request.Topic.NumPartitions, request.Topic.ReplicationFactor); err != nil {
		return nil, err
	}

	return &paustq_proto.CreateTopicResponse{Magic: -1}, nil
}

func (s *TopicServiceServer) DeleteTopic(_ context.Context, request *paustq_proto.DeleteTopicRequest) (*paustq_proto.DeleteTopicResponse, error) {

	if err := s.DB.DeleteTopic(request.TopicName); err != nil {
		return nil, err
	}

	return &paustq_proto.DeleteTopicResponse{Magic: -1}, nil
}

func (s *TopicServiceServer) DescribeTopic(_ context.Context, request *paustq_proto.DescribeTopicRequest) (*paustq_proto.DescribeTopicResponse, error) {

	result, err := s.DB.GetTopic(request.TopicName)

	if err != nil {
		return nil, err
	}

	if result == nil || !result.Exists() {
		return nil, errors.New("topic not exists")
	}
	topicValue := storage.NewTopicValueWithBytes(result.Data())
	topic := &paustq_proto.Topic{TopicName: request.TopicName, TopicMeta: topicValue.TopicMeta(),
		NumPartitions: topicValue.NumPartitions(), ReplicationFactor: topicValue.ReplicationFactor()}
	topicInfo := &paustq_proto.TopicInfo{Topic: topic} // TODO:: temporary topic info

	return &paustq_proto.DescribeTopicResponse{Magic: -1, TopicInfo: topicInfo}, nil
}

func (s *TopicServiceServer) ListTopics(_ context.Context, _ *paustq_proto.ListTopicsRequest) (*paustq_proto.ListTopicsResponse, error) {

	iter := s.DB.Scan(storage.TopicCF)
	iter.SeekToFirst()
	var topics []*paustq_proto.Topic
	for iter.Valid() {
		topicName := string(iter.Key().Data())
		topicValue := storage.NewTopicValueWithBytes(iter.Value().Data())
		topic := &paustq_proto.Topic{TopicName: topicName, TopicMeta: topicValue.TopicMeta(),
			NumPartitions: topicValue.NumPartitions(), ReplicationFactor: topicValue.ReplicationFactor()}
		topics = append(topics, topic)
		iter.Next()
	}

	return &paustq_proto.ListTopicsResponse{Magic: -1, Topics: topics}, nil
}

