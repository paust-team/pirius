package message

import (
	"github.com/elon0823/paustq/proto"
	"github.com/golang/protobuf/proto"
)

func NewConnectMsg(sessionType paustq_proto.SessionType) ([]byte, error) {
	connectReq := &paustq_proto.Connect{
		Magic: -1, SessionType: sessionType,
	}

	return proto.Marshal(connectReq)
}

func NewPutRequestMsg(topic string, data []byte) ([]byte, error) {

	putRequest := &paustq_proto.PutRequest{
		Magic: -1, TopicName: topic, Data: data,
	}

	return proto.Marshal(putRequest)
}

func ParsePutResponseMsg(received []byte) (*paustq_proto.PutResponse, error) {

	putResponse := &paustq_proto.PutResponse{}
	err := proto.Unmarshal(received, putResponse)

	return putResponse, err
}

func NewFetchRequestMsg(topic string, startOffset uint64) ([]byte, error) {

	putRequest := &paustq_proto.FetchRequest{
		Magic: -1, TopicName: topic, StartOffset: startOffset,
	}

	return proto.Marshal(putRequest)
}

func ParseFetchResponseMsg(received []byte) (*paustq_proto.FetchResponse, error) {

	fetchResponse := &paustq_proto.FetchResponse{}
	err := proto.Unmarshal(received, fetchResponse)

	return fetchResponse, err
}