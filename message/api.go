package message

import (
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
)

const MAGIC_NUM int32 = 1101

// API messages
func NewListTopicRequestMsg() *paustqproto.ListTopicRequest {
	return &paustqproto.ListTopicRequest{Magic: MAGIC_NUM}
}

func NewListTopicResponseMsg(topics []string, err pqerror.PQError) *paustqproto.ListTopicResponse {

	response := &paustqproto.ListTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
		return response
	}

	response.Topics = topics
	return response
}

func NewDescribeTopicRequestMsg(topicName string) *paustqproto.DescribeTopicRequest {
	return &paustqproto.DescribeTopicRequest{Magic: MAGIC_NUM, TopicName: topicName}
}

func NewDescribeTopicResponseMsg(topicName, description string, numPartitions, replicationFactor uint32, err pqerror.PQError) *paustqproto.DescribeTopicResponse {

	response := &paustqproto.DescribeTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
		return response
	}

	topic := &paustqproto.Topic{
		Name: topicName, Description: description, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
	}
	response.Topic = topic
	return response
}

func NewCreateTopicRequestMsg(topicName string, description string, numPartitions uint32, replicationFactor uint32) *paustqproto.CreateTopicRequest {
	topic := &paustqproto.Topic{
		Name: topicName, Description: description, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
	}

	return &paustqproto.CreateTopicRequest{Magic: MAGIC_NUM, Topic: topic}
}

func NewCreateTopicResponseMsg(err pqerror.PQError) *paustqproto.CreateTopicResponse {
	response := &paustqproto.CreateTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}

func NewTopicMsg(topicName string, description string, numPartition uint32, replicationFactor uint32) *paustqproto.Topic {
	return &paustqproto.Topic{Name: topicName, Description: description,
		NumPartitions: numPartition, ReplicationFactor: replicationFactor}
}

func NewDeleteTopicRequestMsg(topicName string) *paustqproto.DeleteTopicRequest {
	return &paustqproto.DeleteTopicRequest{Magic: MAGIC_NUM, TopicName: topicName}
}

func NewDeleteTopicResponseMsg(err pqerror.PQError) *paustqproto.DeleteTopicResponse {
	response := &paustqproto.DeleteTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}

func NewPingMsg(msg string, brokerId uint64) *paustqproto.Ping {
	return &paustqproto.Ping{Magic: MAGIC_NUM, Echo: msg, BrokerId: brokerId}
}

func NewPongMsg(msg string, serverVersion uint32, serverTime uint64) *paustqproto.Pong {
	return &paustqproto.Pong{Magic: MAGIC_NUM, Echo: msg, ServerVersion: serverVersion, ServerTime: serverTime}
}

// Stream messages
func NewConnectRequestMsg(sessionType paustqproto.SessionType, topicName string) *paustqproto.ConnectRequest {
	return &paustqproto.ConnectRequest{Magic: MAGIC_NUM, SessionType: sessionType, TopicName: topicName}
}

func NewConnectResponseMsg() *paustqproto.ConnectResponse {
	return &paustqproto.ConnectResponse{Magic: MAGIC_NUM}
}

func NewPutRequestMsg(data []byte) *paustqproto.PutRequest {
	return &paustqproto.PutRequest{Magic: MAGIC_NUM, Data: data}
}

func NewPutResponseMsg() *paustqproto.PutResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.PutResponse{Magic: MAGIC_NUM, Partition: partition}
}

func NewFetchRequestMsg(startOffset uint64) *paustqproto.FetchRequest {
	return &paustqproto.FetchRequest{Magic: MAGIC_NUM, StartOffset: startOffset}
}

func NewFetchResponseMsg(data []byte, lastOffset uint64, offset uint64) *paustqproto.FetchResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.FetchResponse{Magic: MAGIC_NUM, Partition: partition, Data: data, LastOffset: lastOffset, Offset: offset}
}

func NewAckMsg(code uint32, msg string) *paustqproto.Ack {
	return &paustqproto.Ack{Magic: MAGIC_NUM, Code: code, Msg: msg}
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

func NewDiscoverBrokerRequestMsg(topicName string) *paustqproto.DiscoverBrokerRequest {
	return &paustqproto.DiscoverBrokerRequest{Magic: MAGIC_NUM, TopicName: topicName}
}

func NewDiscoverBrokerResponseMsg(addr string, err pqerror.PQError) *paustqproto.DiscoverBrokerResponse {
	response := &paustqproto.DiscoverBrokerResponse{Magic: MAGIC_NUM, Address: addr}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}
