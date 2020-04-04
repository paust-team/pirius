package message

import (
	"errors"
	"github.com/elon0823/paustq/proto"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
)

func PackFrom(msg proto.Message) ([]byte, error) {
	anyMsg, err := ptypes.MarshalAny(msg)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(anyMsg)
}

func UnPackTo(data []byte, msg proto.Message) error {

	anyCont := &any.Any{}
	if err := proto.Unmarshal(data, anyCont); err != nil {
		return err
	}

	if ptypes.Is(anyCont, msg) {
		return ptypes.UnmarshalAny(anyCont, msg)
	} else {
		return errors.New("Not same type: " + anyCont.TypeUrl)
	}
}

func NewConnectMsgData(sessionType paustq_proto.SessionType) ([]byte, error) {
	connectReq := &paustq_proto.Connect{
		Magic: -1, SessionType: sessionType,
	}

	return PackFrom(connectReq)
}

func NewPutRequestMsgData(topic string, data []byte) ([]byte, error) {

	putRequest := &paustq_proto.PutRequest{
		Magic: -1, TopicName: topic, Data: data,
	}

	return PackFrom(putRequest)
}

func NewPutResponseMsgData(topic string, errorCode int32) ([]byte, error) {

	partition := &paustq_proto.Partition{
		PartitionId: 1, Offset: 0,
	}

	putResponse := &paustq_proto.PutResponse{
		Magic: -1, TopicName: topic, Partition: partition, ErrorCode: errorCode,
	}

	return PackFrom(putResponse)
}

func NewFetchRequestMsgData(topic string, startOffset uint64) ([]byte, error) {

	fetchRequest := &paustq_proto.FetchRequest{
		Magic: -1, TopicName: topic, StartOffset: startOffset,
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
