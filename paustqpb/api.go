package paustqpb

import "github.com/golang/protobuf/proto"

func NewPutRequestMsg(topic string, data []byte) ([]byte, error) {

	putRequest := &PutRequest{
		Magic: -1, TopicName: topic, Data: data,
	}

	return proto.Marshal(putRequest)
}

func ParsePutResponseMsg(received []byte) (*PutResponse, error) {

	putResponse := &PutResponse{}
	err := proto.Unmarshal(received, putResponse)

	return putResponse, err
}

func NewFetchRequestMsg(topic string, startOffset uint64) ([]byte, error) {

	putRequest := &FetchRequest{
		Magic: -1, TopicName: topic, StartOffset: startOffset,
	}

	return proto.Marshal(putRequest)
}

func ParseFetchResponseMsg(received []byte) (*FetchResponse, error) {

	fetchResponse := &FetchResponse{}
	err := proto.Unmarshal(received, fetchResponse)

	return fetchResponse, err
}