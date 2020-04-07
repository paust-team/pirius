package message

import (
	"github.com/elon0823/paustq/proto"
)

func NewConnectRequestMsgData(sessionType paustq_proto.SessionType, topic string) ([]byte, error) {
	connectReq := &paustq_proto.ConnectRequest{
		Magic: -1, SessionType: sessionType, TopicName: topic,
	}

	return PackFrom(connectReq)
}

func NewConnectResponseMsgData(errorCode int32) ([]byte, error) {

	putResponse := &paustq_proto.ConnectResponse{
		Magic: -1, ErrorCode: errorCode,
	}

	return PackFrom(putResponse)
}

func NewPutRequestMsgData(data []byte) ([]byte, error) {

	putRequest := &paustq_proto.PutRequest{
		Magic: -1, Data: data,
	}

	return PackFrom(putRequest)
}

func NewPutResponseMsgData(errorCode int32) ([]byte, error) {

	partition := &paustq_proto.Partition{
		PartitionId: 1, Offset: 0,
	}

	putResponse := &paustq_proto.PutResponse{
		Magic: -1, Partition: partition, ErrorCode: errorCode,
	}

	return PackFrom(putResponse)
}

func NewFetchRequestMsgData(startOffset uint64) ([]byte, error) {

	fetchRequest := &paustq_proto.FetchRequest{
		Magic: -1, StartOffset: startOffset,
	}

	return PackFrom(fetchRequest)
}

func NewFetchResponseMsgData(data []byte, errorCode int32) ([]byte, error) {
	partition := &paustq_proto.Partition{
		PartitionId: 1, Offset: 0,
	}

	fetchResponse := &paustq_proto.FetchResponse{
		Magic: -1, Partition: partition, Data: data, ErrorCode: errorCode,
	}

	return PackFrom(fetchResponse)
}
