package message

import (
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
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

func NewDescribeTopicResponseMsg(topicName, description string, replicationFactor uint32, fragmentIds []uint32,
	err pqerror.PQError) *shapleqproto.DescribeTopicResponse {

	response := &shapleqproto.DescribeTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
		return response
	}

	topic := &shapleqproto.TopicInfo{
		Name:              topicName,
		Description:       description,
		ReplicationFactor: replicationFactor,
		FragmentIds:       fragmentIds,
	}
	response.Topic = topic
	return response
}

func NewCreateTopicRequestMsg(topicName string, description string) *shapleqproto.CreateTopicRequest {
	return &shapleqproto.CreateTopicRequest{
		Magic:            MAGIC_NUM,
		TopicName:        topicName,
		TopicDescription: description,
	}
}

func NewCreateTopicResponseMsg(err pqerror.PQError) *shapleqproto.CreateTopicResponse {
	response := &shapleqproto.CreateTopicResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
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

func NewCreateTopicFragmentRequestMsg(topicName string) *shapleqproto.CreateFragmentRequest {
	return &shapleqproto.CreateFragmentRequest{Magic: MAGIC_NUM, TopicName: topicName}
}

func NewCreateTopicFragmentResponseMsg(fragmentId uint32, err pqerror.PQError) *shapleqproto.CreateFragmentResponse {
	response := &shapleqproto.CreateFragmentResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	} else {
		response.Fragment = &shapleqproto.FragmentInfo{
			Id:         fragmentId,
			LastOffset: 0,
		}
	}
	return response
}

func NewDeleteTopicFragmentRequestMsg(topicName string, fragmentId uint32) *shapleqproto.DeleteFragmentRequest {
	return &shapleqproto.DeleteFragmentRequest{Magic: MAGIC_NUM, TopicName: topicName, FragmentId: fragmentId}
}

func NewDeleteTopicFragmentResponseMsg(err pqerror.PQError) *shapleqproto.DeleteFragmentResponse {
	response := &shapleqproto.DeleteFragmentResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	}
	return response
}

func NewDescribeTopicFragmentRequestMsg(topicName string, fragmentId uint32) *shapleqproto.DescribeFragmentRequest {
	return &shapleqproto.DescribeFragmentRequest{Magic: MAGIC_NUM, TopicName: topicName, FragmentId: fragmentId}
}

func NewDescribeTopicFragmentResponseMsg(fragmentId uint32, lastOffset uint64, brokerAddresses []string,
	err pqerror.PQError) *shapleqproto.DescribeFragmentResponse {

	response := &shapleqproto.DescribeFragmentResponse{Magic: MAGIC_NUM}
	if err != nil {
		response.ErrorCode = int32(err.Code())
		response.ErrorMessage = err.Error()
	} else {
		response.Fragment = &shapleqproto.FragmentInfo{
			Id:              fragmentId,
			LastOffset:      lastOffset,
			BrokerAddresses: brokerAddresses,
		}
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

func NewConnectRequestMsg(sessionType shapleqproto.SessionType, topicTargets []*common.TopicFragments) *shapleqproto.ConnectRequest {
	var targets []*shapleqproto.TopicFragmentsTarget
	for _, topicObject := range topicTargets {
		var offsets []*shapleqproto.TopicFragmentsTarget_FragmentOffset
		for fragmentId := range topicObject.FragmentOffsets() {
			offsets = append(offsets, &shapleqproto.TopicFragmentsTarget_FragmentOffset{FragmentId: fragmentId})
		}

		targets = append(targets, &shapleqproto.TopicFragmentsTarget{
			TopicName: topicObject.Topic(),
			Offsets:   offsets,
		})
	}
	return &shapleqproto.ConnectRequest{Magic: MAGIC_NUM, SessionType: sessionType, TopicTargets: targets}
}

func NewConnectResponseMsg() *shapleqproto.ConnectResponse {
	return &shapleqproto.ConnectResponse{Magic: MAGIC_NUM}
}

func NewPutRequestMsg(data []byte, seqNum uint64, nodeId string, topicName string, fragmentId uint32) *shapleqproto.PutRequest {
	return &shapleqproto.PutRequest{Magic: MAGIC_NUM, Data: data, SeqNum: seqNum, NodeId: nodeId, TopicName: topicName, FragmentId: fragmentId}
}

func NewPutResponseMsg(topicName string, fragmentId uint32, offset uint64) *shapleqproto.PutResponse {
	return &shapleqproto.PutResponse{Magic: MAGIC_NUM, TopicName: topicName, FragmentId: fragmentId, LastOffset: offset}
}

func NewFetchRequestMsg(topicFragments []*common.TopicFragments, maxBatchSize uint32, flushInterval uint32) *shapleqproto.FetchRequest {
	var topicTargets []*shapleqproto.TopicFragmentsTarget
	for _, topicObject := range topicFragments {
		var offsets []*shapleqproto.TopicFragmentsTarget_FragmentOffset
		for fragmentId, startOffset := range topicObject.FragmentOffsets() {
			offsets = append(offsets, &shapleqproto.TopicFragmentsTarget_FragmentOffset{
				FragmentId:  fragmentId,
				StartOffset: startOffset,
			})
		}

		topicTargets = append(topicTargets, &shapleqproto.TopicFragmentsTarget{
			TopicName: topicObject.Topic(),
			Offsets:   offsets,
		})
	}
	return &shapleqproto.FetchRequest{Magic: MAGIC_NUM, TopicTargets: topicTargets, MaxBatchSize: maxBatchSize,
		FlushInterval: flushInterval}
}

func NewFetchResponseMsg(data []byte, offset uint64, seqNum uint64, nodeId string, topicName string, lastOffset uint64, fragmentId uint32) *shapleqproto.FetchResponse {
	return &shapleqproto.FetchResponse{Data: data, Offset: offset, SeqNum: seqNum, NodeId: nodeId, LastOffset: lastOffset, FragmentId: fragmentId, TopicName: topicName}
}

func NewBatchFetchResponseMsg(batched []*shapleqproto.FetchResponse) *shapleqproto.BatchedFetchResponse {
	var items []*shapleqproto.BatchedFetchResponse_Fetched
	var lastOffset uint64 = 0

	for _, fetched := range batched {
		if lastOffset < fetched.LastOffset {
			lastOffset = fetched.LastOffset
		}
		items = append(items, &shapleqproto.BatchedFetchResponse_Fetched{
			Data:       fetched.Data,
			Offset:     fetched.Offset,
			SeqNum:     fetched.SeqNum,
			NodeId:     fetched.NodeId,
			TopicName:  fetched.TopicName,
			FragmentId: fetched.FragmentId,
		})
	}
	return &shapleqproto.BatchedFetchResponse{Magic: MAGIC_NUM, Items: items, LastOffset: lastOffset}
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
