package message

import (
	"fmt"
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

type fn func(msg proto.Message, args ...interface{})
type Handler struct {
	messageMap map[proto.Message]fn
}

func (h *Handler) RegisterMsgHandle(msg proto.Message, f fn) {
	if h.messageMap == nil {
		h.messageMap = make(map[proto.Message]fn)
	}

	h.messageMap[msg] = f
}

func (h *Handler) Handle(qMsg *QMessage, args ...interface{}) error {
	isHandled := false
	for msg, fn := range h.messageMap {
		if qMsg.Is(msg) {
			newMsg := msg
			if err := qMsg.UnpackTo(newMsg); err != nil {
				return err
			}
			fn(newMsg, args...)
			isHandled = true
			break
		}
	}
	if !isHandled {
		return pqerror.UnhandledError{ErrStr: fmt.Sprintf("no handler exists for %s", qMsg.Data)}
	}
	return nil
}
