package paustqpb

import "github.com/golang/protobuf/proto"

func NewPutRequestMsg(topic string, data []byte) ([]byte, error) {

	putRequest := &PutRequest{
		Magic: -1, TopicName: topic, Data: data,
	}

	return proto.Marshal(putRequest)
}

func ParseResponseMsg(received []byte) (*PutResponse, error) {

	putResponse := &PutResponse{}
	err := proto.Unmarshal(received, putResponse)

	return putResponse, err
}