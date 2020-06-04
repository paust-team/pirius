package network

import (
	"encoding/binary"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	"hash/crc32"
)

type Header struct {
	len      uint32
	checksum uint32
	msgType  uint16
}

const (
	TRANSACTION uint16 = 0
	STREAM      uint16 = 1
)

func (h *Header) deserializeFrom(data []byte) error {
	headerSize := binary.Size(h)
	if len(data) < headerSize {
		return pqerror.NotEnoughBufferError{}
	}

	lenSize := binary.Size(h.len)
	checksumSize := binary.Size(h.checksum)

	h.len = binary.BigEndian.Uint32(data[0:lenSize])
	h.checksum = binary.BigEndian.Uint32(data[lenSize : lenSize+checksumSize])
	h.msgType = binary.BigEndian.Uint16(data[lenSize+checksumSize : headerSize])

	if len(data) < int(h.len) {
		return pqerror.NotEnoughBufferError{}
	}

	checksum := crc32.ChecksumIEEE(data[headerSize : uint32(headerSize)+h.len])
	if h.checksum != checksum {
		return pqerror.InvalidChecksumError{}
	}

	return nil
}

func (h *Header) serializeTo(buffer []byte) error {
	if len(buffer) < binary.Size(h) {
		return pqerror.NotEnoughBufferError{}
	}

	lenSize := binary.Size(h.len)
	checksumSize := binary.Size(h.checksum)
	headerSize := binary.Size(h)

	binary.BigEndian.PutUint32(buffer[0:lenSize], h.len)
	binary.BigEndian.PutUint32(buffer[lenSize:lenSize+checksumSize], h.checksum)
	binary.BigEndian.PutUint16(buffer[lenSize+checksumSize:headerSize], h.msgType)

	return nil
}

func Deserialize(data []byte) (*message.QMessage, error) {
	header := &Header{}
	if err := header.deserializeFrom(data); err != nil {
		return nil, err
	}

	headerSize := binary.Size(header)
	msgData := data[headerSize : headerSize+int(header.len)]
	return message.NewQMessage(header.msgType, msgData), nil
}

func Serialize(msg *message.QMessage) ([]byte, error) {
	checksum := crc32.ChecksumIEEE(msg.Data)
	msgLen := uint32(len(msg.Data))
	header := &Header{checksum: checksum, len: msgLen, msgType: msg.Type()}
	headerSize := uint32(binary.Size(header))

	serialized := make([]byte, headerSize, headerSize+msgLen)

	if err := header.serializeTo(serialized); err != nil {
		return nil, err
	}

	serialized = append(serialized, msg.Data...)
	return serialized, nil
}
