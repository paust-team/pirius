package message

import (
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"hash/crc32"
	"unsafe"
)

type Header struct {
	Len      uint32
	Checksum uint32
}

func (header *Header) DeserializeFrom(data []byte) error {

	if len(data) < int(unsafe.Sizeof(header)) {
		return errors.New("size of buffer is smaller then size of header")
	}

	header.Len = binary.BigEndian.Uint32(data[0:4])
	header.Checksum = binary.BigEndian.Uint32(data[4:8])

	return nil
}

func (header *Header) SerializeTo(buffer []byte) error {

	if len(buffer) < int(unsafe.Sizeof(header)) {
		return errors.New("size of buffer is smaller then size of header")
	}

	binary.BigEndian.PutUint32(buffer[0:4], header.Len)
	binary.BigEndian.PutUint32(buffer[4:8], header.Checksum)

	return nil
}

func PackFrom(msg proto.Message) ([]byte, error) {
	anyMsg, err := ptypes.MarshalAny(msg)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(anyMsg)
}

func UnPackTo(data []byte, msg proto.Message) error {

	anyCont := &any.Any{}
	if err := proto.Unmarshal(data, anyCont); err != nil {
		return err
	}

	if ptypes.Is(anyCont, msg) {
		return ptypes.UnmarshalAny(anyCont, msg)
	} else {
		return errors.New("Not same type: " + anyCont.TypeUrl)
	}
}

func Deserialize(data []byte) ([]byte, error) {

	header := &Header{}
	if err := header.DeserializeFrom(data); err != nil {
		return nil, err
	}

	msgData := data[unsafe.Sizeof(header):]
	if uint32(len(msgData)) < header.Len {
		return msgData, errors.New("more chunk required")
	}

	checksum := crc32.ChecksumIEEE(msgData)
	if header.Checksum == checksum {
		return msgData, nil
	}
	return nil, errors.New("checksum failed")
}

func Serialize(data []byte) ([]byte, error) {

	checksum := crc32.ChecksumIEEE(data)
	msgLen := uint32(len(data))
	header := &Header{Checksum: checksum, Len: msgLen}
	headerSize := uint32(unsafe.Sizeof(header))

	serialized := make([]byte, headerSize, headerSize+msgLen)
	if err := header.SerializeTo(serialized); err != nil {
		return nil, err
	}

	serialized = append(serialized, data...)
	return serialized, nil
}
