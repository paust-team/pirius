package network

import (
	"encoding/binary"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	"hash/crc32"
)

type Header struct {
	len      uint32
	checksum uint32
	msgType  uint16
}

var (
	emptyHeader  = &Header{}
	headerSize   = binary.Size(emptyHeader)
	lenSize      = binary.Size(emptyHeader.len)
	checksumSize = binary.Size(emptyHeader.checksum)
)

var (
	lenIdx      = lenSize
	checksumIdx = lenIdx + checksumSize
	msgTypeIdx  = headerSize
)

func (h *Header) deserializeFrom(data []byte) error {
	dataLen := len(data)
	dataMessageLen := dataLen - headerSize

	if dataLen < headerSize {
		return pqerror.NotEnoughBufferError{}
	}

	h.len = binary.BigEndian.Uint32(data[0:lenIdx])
	h.checksum = binary.BigEndian.Uint32(data[lenIdx:checksumIdx])
	h.msgType = binary.BigEndian.Uint16(data[checksumIdx:msgTypeIdx])

	messageLen := int(h.len)
	if dataMessageLen < messageLen {
		return pqerror.NotEnoughBufferError{}
	}

	checksum := crc32.ChecksumIEEE(data[headerSize : headerSize+int(h.len)])
	if h.checksum != checksum {
		return pqerror.InvalidChecksumError{}
	}
	return nil
}

func (h *Header) serializeTo(buffer []byte) error {
	if len(buffer) < headerSize {
		return pqerror.NotEnoughBufferError{}
	}

	binary.BigEndian.PutUint32(buffer[0:lenIdx], h.len)
	binary.BigEndian.PutUint32(buffer[lenIdx:checksumIdx], h.checksum)
	binary.BigEndian.PutUint16(buffer[checksumIdx:msgTypeIdx], h.msgType)

	return nil
}

func Deserialize(data []byte) (*message.QMessage, error) {
	header := &Header{}
	if err := header.deserializeFrom(data); err != nil {
		return nil, err
	}

	msgData := data[headerSize : headerSize+int(header.len)]
	return message.NewQMessage(header.msgType, msgData), nil
}

func Serialize(msg *message.QMessage) ([]byte, error) {
	checksum := crc32.ChecksumIEEE(msg.Data)
	msgLen := uint32(len(msg.Data))
	header := &Header{checksum: checksum, len: msgLen, msgType: msg.Type()}

	serialized := make([]byte, headerSize, headerSize+int(msgLen))

	if err := header.serializeTo(serialized); err != nil {
		return nil, err
	}

	serialized = append(serialized, msg.Data...)
	return serialized, nil
}
