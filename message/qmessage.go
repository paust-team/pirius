package message

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/paust-team/paustq/pqerror"
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
		return nil, pqerror.UnmarshalFailedError{}
	}

	return anyMsg, nil
}

func (q *QMessage) UnpackTo(msg proto.Message) error {
	anyMsg, err := q.Any()
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(q.Data, anyMsg); err != nil {
		return pqerror.UnmarshalFailedError{}
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
