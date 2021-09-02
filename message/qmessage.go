package message

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/paust-team/shapleq/pqerror"
	"hash/crc32"
)

// message type
const (
	TRANSACTION uint16 = 0
	STREAM      uint16 = 1
)

type qHeader struct {
	len      uint32
	checksum uint32
	msgType  uint16
}

var (
	emptyHeader  = &qHeader{}
	HeaderSize   = binary.Size(emptyHeader)
	lenSize      = binary.Size(emptyHeader.len)
	checksumSize = binary.Size(emptyHeader.checksum)
)

var (
	lenIdx      = lenSize
	checksumIdx = lenIdx + checksumSize
	msgTypeIdx  = HeaderSize
)

type QMessage struct {
	header qHeader
	Data   []byte
}

func NewQMessage(buf []byte) (*QMessage, error) {
	bufLen := len(buf)

	// check header
	if bufLen < HeaderSize {
		return nil, pqerror.NotEnoughBufferError{}
	}

	actualDataLen := binary.BigEndian.Uint32(buf[0:lenIdx])
	actualChecksum := binary.BigEndian.Uint32(buf[lenIdx:checksumIdx])
	msgType := binary.BigEndian.Uint16(buf[checksumIdx:msgTypeIdx])

	// check data
	receivedDataLen := bufLen - HeaderSize
	if receivedDataLen < int(actualDataLen) {
		return nil, pqerror.NotEnoughBufferError{}
	}

	data := buf[HeaderSize : HeaderSize+int(actualDataLen)]
	if actualChecksum != crc32.ChecksumIEEE(data) {
		return nil, pqerror.InvalidChecksumError{}
	}

	return &QMessage{
		header: qHeader{checksum: actualChecksum, len: actualDataLen, msgType: msgType},
		Data:   data,
	}, nil
}

func NewQMessageFromMsg(msgType uint16, msg proto.Message) (*QMessage, error) {
	anyMsg, err := ptypes.MarshalAny(msg)
	if err != nil {
		return nil, pqerror.MarshalAnyFailedError{}
	}
	data, err := proto.Marshal(anyMsg)
	if err != nil {
		return nil, pqerror.MarshalFailedError{}
	}

	return &QMessage{
		header: qHeader{checksum: crc32.ChecksumIEEE(data), len: uint32(len(data)), msgType: msgType},
		Data:   data,
	}, nil
}

func (q *QMessage) Serialize() ([]byte, error) {
	serialized := make([]byte, HeaderSize, HeaderSize+int(q.header.len))

	binary.BigEndian.PutUint32(serialized[0:lenIdx], q.header.len)
	binary.BigEndian.PutUint32(serialized[lenIdx:checksumIdx], q.header.checksum)
	binary.BigEndian.PutUint16(serialized[checksumIdx:msgTypeIdx], q.header.msgType)

	serialized = append(serialized, q.Data...)
	return serialized, nil
}

func (q *QMessage) Length() uint32 {
	return q.header.len
}

func (q *QMessage) Type() uint16 {
	return q.header.msgType
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
