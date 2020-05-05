package message

import (
	paustqproto "github.com/paust-team/paustq/proto"
)

func NewCreateTopicRequestMsg(topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) *paustqproto.CreateTopicRequest {
	topic := &paustqproto.Topic{
		TopicName: topicName, TopicMeta: topicMeta, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
	}

	return &paustqproto.CreateTopicRequest{Topic: topic}
}

func NewCreateTopicResponseMsg() *paustqproto.CreateTopicResponse {
	return &paustqproto.CreateTopicResponse{}
}

func NewTopicMsg(topicName string, topicMeta string, numPartition uint32, replicationFactor uint32) *paustqproto.Topic {
	return &paustqproto.Topic{TopicName: topicName, TopicMeta: topicMeta,
		NumPartitions: numPartition, ReplicationFactor: replicationFactor}
}

func NewDeleteTopicRequestMsg(topicName string) *paustqproto.DeleteTopicRequest {
	return &paustqproto.DeleteTopicRequest{TopicName: topicName}
}

func NewDeleteTopicResponseMsg() *paustqproto.DeleteTopicResponse {
	return &paustqproto.DeleteTopicResponse{}
}

func NewConnectRequestMsg(sessionType paustqproto.SessionType, topicName string) *paustqproto.ConnectRequest {
	return &paustqproto.ConnectRequest{SessionType: sessionType, TopicName: topicName}
}

func NewConnectResponseMsg() *paustqproto.ConnectResponse {
	return &paustqproto.ConnectResponse{}
}

func NewPutRequestMsg(data []byte) *paustqproto.PutRequest {
	return &paustqproto.PutRequest{Data: data}
}

func NewPutResponseMsg() *paustqproto.PutResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.PutResponse{Partition: partition}
}

func NewFetchRequestMsg(startOffset uint64) *paustqproto.FetchRequest {
	return &paustqproto.FetchRequest{StartOffset: startOffset}
}

func NewFetchResponseMsg(data []byte, lastOffset uint64, offset uint64) *paustqproto.FetchResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.FetchResponse{Partition: partition, Data: data, LastOffset: lastOffset, Offset: offset}
}

func NewPingMsg(msg string, brokerId uint64) *paustqproto.Ping {
	return &paustqproto.Ping{Echo: msg, BrokerId: brokerId}
}

func NewPongMsg(msg string, serverVersion uint32, serverTime uint64) *paustqproto.Pong {
	return &paustqproto.Pong{Echo: msg, ServerVersion: serverVersion, ServerTime: serverTime}
}

func NewShutdownBrokerRequestMsg(brokerId uint64) *paustqproto.ShutdownBrokerRequest {
	return &paustqproto.ShutdownBrokerRequest{BrokerId: brokerId}
}

func NewShutdownBrokerResponseMsg() *paustqproto.ShutdownBrokerResponse {
	return &paustqproto.ShutdownBrokerResponse{}
}
