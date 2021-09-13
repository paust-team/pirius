package message

import (
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
)

const MAGIC_NUM int32 = 1101

// API messages
func NewListTopicRequestMsg() *shapleqproto.ListTopicRequest {
	return &shapleqproto.ListTopicRequest{Magic: MAGIC_NUM}
}

func NewListTopicResponseMsg(topics []string, err pqerror.PQError) *shapleqproto.ListTopicResponse {

	response := &shapleqproto.ListTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
		return response
	}

	response.Topics = topics
	return response
}

func NewDescribeTopicRequestMsg(topicName string) *shapleqproto.DescribeTopicRequest {
	return &shapleqproto.DescribeTopicRequest{Magic: MAGIC_NUM, TopicName: topicName}
}

func NewDescribeTopicResponseMsg(topicName, description string, numPartitions, replicationFactor uint32, err pqerror.PQError) *shapleqproto.DescribeTopicResponse {

	response := &shapleqproto.DescribeTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
		return response
	}

	topic := &shapleqproto.Topic{
		Name: topicName, Description: description, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
	}
	response.Topic = topic
	return response
}

func NewCreateTopicRequestMsg(topicName string, description string, numPartitions uint32, replicationFactor uint32) *shapleqproto.CreateTopicRequest {
	topic := &shapleqproto.Topic{
		Name: topicName, Description: description, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
	}

	return &shapleqproto.CreateTopicRequest{Magic: MAGIC_NUM, Topic: topic}
}

func NewCreateTopicResponseMsg(err pqerror.PQError) *shapleqproto.CreateTopicResponse {
	response := &shapleqproto.CreateTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}

func NewTopicMsg(topicName string, description string, numPartition uint32, replicationFactor uint32) *shapleqproto.Topic {
	return &shapleqproto.Topic{Name: topicName, Description: description,
		NumPartitions: numPartition, ReplicationFactor: replicationFactor}
}

func NewDeleteTopicRequestMsg(topicName string) *shapleqproto.DeleteTopicRequest {
	return &shapleqproto.DeleteTopicRequest{Magic: MAGIC_NUM, TopicName: topicName}
}

func NewDeleteTopicResponseMsg(err pqerror.PQError) *shapleqproto.DeleteTopicResponse {
	response := &shapleqproto.DeleteTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}

func NewPingMsg(msg string, brokerId uint64) *shapleqproto.Ping {
	return &shapleqproto.Ping{Magic: MAGIC_NUM, Echo: msg, BrokerId: brokerId}
}

func NewPongMsg(msg string, serverVersion uint32, serverTime uint64) *shapleqproto.Pong {
	return &shapleqproto.Pong{Magic: MAGIC_NUM, Echo: msg, ServerVersion: serverVersion, ServerTime: serverTime}
}

// Stream messages
func NewConnectRequestMsg(sessionType shapleqproto.SessionType, topicName string) *shapleqproto.ConnectRequest {
	return &shapleqproto.ConnectRequest{Magic: MAGIC_NUM, SessionType: sessionType, TopicName: topicName}
}

func NewConnectResponseMsg() *shapleqproto.ConnectResponse {
	return &shapleqproto.ConnectResponse{Magic: MAGIC_NUM}
}

func NewPutRequestMsg(data []byte) *shapleqproto.PutRequest {
	return &shapleqproto.PutRequest{Magic: MAGIC_NUM, Data: data}
}

func NewPutResponseMsg(offset uint64) *shapleqproto.PutResponse {
	partition := &shapleqproto.Partition{
		PartitionId: 1, Offset: offset,
	}

	return &shapleqproto.PutResponse{Magic: MAGIC_NUM, Partition: partition}
}

func NewFetchRequestMsg(startOffset uint64, maxBatchSize uint32, flushInterval uint32) *shapleqproto.FetchRequest {
	return &shapleqproto.FetchRequest{Magic: MAGIC_NUM, StartOffset: startOffset, MaxBatchSize: maxBatchSize,
		FlushInterval: flushInterval}
}

func NewFetchResponseMsg(data []byte, lastOffset uint64, offset uint64) *shapleqproto.FetchResponse {
	partition := &shapleqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &shapleqproto.FetchResponse{Magic: MAGIC_NUM, Partition: partition, Data: data, LastOffset: lastOffset, Offset: offset}
}

func NewBatchFetchResponseMsg(batched [][]byte, lastOffset uint64) *shapleqproto.BatchFetchResponse {

	return &shapleqproto.BatchFetchResponse{Magic: MAGIC_NUM, Batched: batched, LastOffset: lastOffset}
}

func NewAckMsg(code uint32, msg string) *shapleqproto.Ack {
	return &shapleqproto.Ack{Magic: MAGIC_NUM, Code: code, Msg: msg}
}

func NewErrorAckMsg(code pqerror.PQCode, hint string) *QMessage {
	var ackMsg *QMessage
	if code == pqerror.ErrInternal {
		ackMsg, _ = NewQMessageFromMsg(STREAM, NewAckMsg(uint32(code), "broker internal error"))
	} else {
		ackMsg, _ = NewQMessageFromMsg(STREAM, NewAckMsg(uint32(code), hint))
	}
	return ackMsg
}

func NewDiscoverBrokerRequestMsg(topicName string, sessionType shapleqproto.SessionType) *shapleqproto.DiscoverBrokerRequest {
	return &shapleqproto.DiscoverBrokerRequest{Magic: MAGIC_NUM, TopicName: topicName, SessionType: sessionType}
}

func NewDiscoverBrokerResponseMsg(addr string, err pqerror.PQError) *shapleqproto.DiscoverBrokerResponse {
	response := &shapleqproto.DiscoverBrokerResponse{Magic: MAGIC_NUM, Address: addr}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}
