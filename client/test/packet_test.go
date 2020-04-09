package test

import (
	"bytes"
	"github.com/paust-team/paustq/message"
	"hash/crc32"
	"testing"
)

func TestPacketHeader(t *testing.T) {

	testByte := []byte{1, 2, 3, 4, 5}
	expectedLen := uint32(len(testByte))
	expectedChecksum := crc32.ChecksumIEEE(testByte)

	serializedData, err := message.Serialize(testByte)
	if err != nil {
		t.Error(err)
		return
	}

	header := &message.Header{}
	if header.DeserializeFrom(serializedData) != nil {
		t.Error(err)
		return
	}

	if expectedLen != header.Len {
		t.Error("Length is not matched")
	}

	if expectedChecksum != header.Checksum {
		t.Error("Checksum is not matched")
	}
}

func TestPacket(t *testing.T) {

	testByte := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}

	serializedData, err := message.Serialize(testByte)
	if err != nil {
		t.Error(err)
		return
	}

	deserializedData, err := message.Deserialize(serializedData)

	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(testByte, deserializedData) != 0 {
		t.Error("Bytes are not equal")
	}
}
