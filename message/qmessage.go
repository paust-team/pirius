package message

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/paust-team/paustq/pqerror"
)

type QMessage struct {
	msgType uint16
	Data    []byte
}

func NewQMessage(msgType uint16, data []byte) *QMessage {
	return &QMessage{msgType: msgType, Data: data}
}

func NewQMessageFromMsg(msg proto.Message) (*QMessage, error) {
	qMessage := &QMessage{}
	if err := qMessage.PackFrom(msg); err != nil {
		return nil, err
	}

	return qMessage, nil
}

func (q *QMessage) Type() uint16 {
	return q.msgType
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
		return nil, pqerror.UnmarshalFailedError{}
	}

	return anyMsg, nil
}

func (q *QMessage) UnpackTo(msg proto.Message) error {
	anyMsg, err := q.Any()
	if err != nil {
		return err
	}

	if ptypes.Is(anyMsg, msg) {
		err := ptypes.UnmarshalAny(anyMsg, msg)
		if err != nil {
			return pqerror.UnmarshalAnyFailedError{}
		}
	} else {
		return pqerror.InvalidMsgTypeToUnpackError{Type: anyMsg.TypeUrl}
	}
	return nil
}

func (q *QMessage) UnpackAs(msg proto.Message) (proto.Message, error) {
	anyMsg, err := q.Any()
	if err != nil {
		return nil, err
	}

	if ptypes.Is(anyMsg, msg) {
		err := ptypes.UnmarshalAny(anyMsg, msg)
		if err != nil {
			return nil, pqerror.UnmarshalAnyFailedError{}
		}
	} else {
		return nil, pqerror.InvalidMsgTypeToUnpackError{Type: anyMsg.TypeUrl}
	}
	return msg, nil
}

func (q *QMessage) PackFrom(msg proto.Message) error {
	anyMsg, err := ptypes.MarshalAny(msg)
	if err != nil {
		return pqerror.MarshalAnyFailedError{}
	}
	data, err := proto.Marshal(anyMsg)
	if err != nil {
		return pqerror.MarshalFailedError{}
	}
	q.Data = data
	return nil
}