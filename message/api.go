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

func NewConnectRequestMsg(sessionType paustqproto.SessionType, topicName string) *paustqproto.ConnectRequest {
	return &paustqproto.ConnectRequest{Magic: -1, SessionType: sessionType, TopicName: topicName}
}

func NewConnectResponseMsg() *paustqproto.ConnectResponse {
	return &paustqproto.ConnectResponse{Magic: -1}
}

func NewPutRequestMsg(data []byte) *paustqproto.PutRequest {
	return &paustqproto.PutRequest{Magic: -1, Data: data}
}

func NewPutResponseMsg() *paustqproto.PutResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.PutResponse{Magic: -1, Partition: partition}
}

func NewFetchRequestMsg(startOffset uint64) *paustqproto.FetchRequest {
	return &paustqproto.FetchRequest{Magic: -1, StartOffset: startOffset}
}

func NewFetchResponseMsg(data []byte, lastOffset uint64, offset uint64) *paustqproto.FetchResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.FetchResponse{Magic: -1, Partition: partition, Data: data, LastOffset: lastOffset, Offset: offset}
}

func NewPingMsg(msg string, brokerId uint64) *paustqproto.Ping {
	return &paustqproto.Ping{Magic: -1, Echo: msg, BrokerId: brokerId}
}

func NewPongMsg(msg string, serverVersion uint32, serverTime uint64) *paustqproto.Pong{
	return &paustqproto.Pong{Magic: -1, Echo: msg, ServerVersion: serverVersion, ServerTime: serverTime}
}

func NewShutdownBrokerRequestMsg(brokerId uint64) *paustqproto.ShutdownBrokerRequest {
	return &paustqproto.ShutdownBrokerRequest{Magic: -1, BrokerId: brokerId}
}

func NewShutdownBrokerResponseMsg() *paustqproto.ShutdownBrokerResponse {
	return &paustqproto.ShutdownBrokerResponse{Magic: -1}
}
