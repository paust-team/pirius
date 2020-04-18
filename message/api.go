package message

import (
	paustqproto "github.com/paust-team/paustq/proto"
)

func NewConnectRequestMsg(sessionType paustqproto.SessionType, topic string) *paustqproto.ConnectRequest {

	return &paustqproto.ConnectRequest{
		Magic: -1, SessionType: sessionType, TopicName: topic,
	}
}

func NewConnectResponseMsg() *paustqproto.ConnectResponse {

	return &paustqproto.ConnectResponse{
		Magic: -1,
	}
}

func NewPutRequestMsg(data []byte) *paustqproto.PutRequest {

	return &paustqproto.PutRequest{
		Magic: -1, Data: data,
	}
}

func NewPutResponseMsg() *paustqproto.PutResponse {

	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.PutResponse{
		Magic: -1, Partition: partition,
	}
}

func NewFetchRequestMsg(startOffset uint64) *paustqproto.FetchRequest {

	return &paustqproto.FetchRequest{
		Magic: -1, StartOffset: startOffset,
	}
}

func NewFetchResponseMsg(data []byte, lastOffset uint64) *paustqproto.FetchResponse {
	partition := &paustqproto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustqproto.FetchResponse{
		Magic: -1, Partition: partition, Data: data, LastOffset: lastOffset,
	}
}
