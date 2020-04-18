package message

import (
	paustqproto "github.com/paust-team/paustq/proto"
)

func NewCreateTopicRequestMsg(topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) *paustqproto.CreateTopicRequest {
	topic := &paustqproto.Topic{
		TopicName: topicName, TopicMeta: topicMeta, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
	}

	return &paustqproto.CreateTopicRequest{Magic: -1, Topic: topic}
}

func NewCreateTopicResponseMsg() *paustqproto.CreateTopicResponse {
	return &paustqproto.CreateTopicResponse{Magic: -1}
}

func NewTopicMsg(topicName string, topicMeta string, numPartition uint32, replicationFactor uint32) *paustqproto.Topic {
	return &paustqproto.Topic{TopicName: topicName, TopicMeta: topicMeta,
		NumPartitions: numPartition, ReplicationFactor: replicationFactor}
}

func NewDeleteTopicRequestMsg(topicName string) *paustqproto.DeleteTopicRequest{
	return &paustqproto.DeleteTopicRequest{Magic: -1, TopicName: topicName}
}

func NewDeleteTopicResponseMsg() *paustqproto.DeleteTopicResponse {
	return &paustqproto.DeleteTopicResponse{Magic: -1}
}

func NewListTopicsRequestMsg() *paustqproto.ListTopicsRequest {
	return &paustqproto.ListTopicsRequest{Magic: -1}
}

func NewListTopicsResponseMsg(topics []*paustqproto.Topic) *paustqproto.ListTopicsResponse {
	return &paustqproto.ListTopicsResponse{Magic: -1, Topics: topics}
}

func NewDescribeTopicRequestMsg(topicName string) *paustqproto.DescribeTopicRequest {
	return &paustqproto.DescribeTopicRequest{Magic: -1, TopicName: topicName}
}

func NewDescribeTopicResponseMsg(topic *paustqproto.Topic, numPublishers uint64, numSubscribers uint64) *paustqproto.DescribeTopicResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}
	partitions := []*paustqproto.Partition{partition}
	return &paustqproto.DescribeTopicResponse{
		Magic: -1, Topic: topic, NumPublishers: numPublishers, NumSubscribers: numSubscribers, Partitions: partitions}
}

