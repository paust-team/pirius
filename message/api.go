package message

import (
	"github.com/paust-team/paustq/proto"
)

func NewConnectRequestMsg(sessionType paustq_proto.SessionType, topic string) *paustq_proto.ConnectRequest {

	return &paustq_proto.ConnectRequest{
		Magic: -1, SessionType: sessionType, TopicName: topic,
	}
}

func NewConnectResponseMsg(errorCode int32) *paustq_proto.ConnectResponse {

	return &paustq_proto.ConnectResponse{
		Magic: -1, ErrorCode: errorCode,
	}
}

func NewPutRequestMsg(data []byte) *paustq_proto.PutRequest {

	return &paustq_proto.PutRequest{
		Magic: -1, Data: data,
	}
}

func NewPutResponseMsg(errorCode int32) *paustq_proto.PutResponse {

	partition := &paustq_proto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustq_proto.PutResponse{
		Magic: -1, Partition: partition, ErrorCode: errorCode,
	}
}

func NewFetchRequestMsg(startOffset uint64) *paustq_proto.FetchRequest {

	return &paustq_proto.FetchRequest{
		Magic: -1, StartOffset: startOffset,
	}
}

func NewFetchResponseMsg(data []byte, errorCode int32) *paustq_proto.FetchResponse {
	partition := &paustq_proto.Partition{
		PartitionId: 1, Offset: 0,
	}

	return &paustq_proto.FetchResponse{
		Magic: -1, Partition: partition, Data: data, ErrorCode: errorCode,
	}
}
