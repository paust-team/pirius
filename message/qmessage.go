package message

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
)

type QMessage struct {
	Data []byte
}

func NewQMessage(data []byte) *QMessage {
	return &QMessage{Data: data}
}

func NewQMessageFromMsg(msg proto.Message) (*QMessage, error) {
	qMessage := &QMessage{}
	if err := qMessage.PackFrom(msg); err != nil {
		return nil, err
	}

	return qMessage, nil
}

func (q *QMessage) Is(msg proto.Message) bool {
	anyMsg, err := q.Any()
	if err != nil {
		return false
	}
	return ptypes.Is(anyMsg, msg)
}

func (q *QMessage) Any() (*any.Any, error) {
	anyMsg := &any.Any{}
	if err := proto.Unmarshal(q.Data, anyMsg); err != nil {
		return nil, err
	}

	return anyMsg, nil
}

func (q *QMessage) UnpackTo(msg proto.Message) error {
	anyMsg, err := q.Any()
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(q.Data, anyMsg); err != nil {
		return err
	}

	if ptypes.Is(anyMsg, msg) {
		return ptypes.UnmarshalAny(anyMsg, msg)
	} else {
		return errors.New("Not same type: " + anyMsg.TypeUrl)
	}
}

func (q *QMessage) PackFrom(msg proto.Message) error {
	anyMsg, err := ptypes.MarshalAny(msg)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(anyMsg)
	if err != nil {
		return err
	}
	q.Data = data
	return nil
}
