package common

import (
	"encoding/binary"
	"unsafe"
)

type TopicData struct {
	data []byte
}

var uint32Len = int(unsafe.Sizeof(uint32(0)))
var uint64Len = int(unsafe.Sizeof(uint64(0)))

func NewTopicData(data []byte) *TopicData {
	return &TopicData{data: data}
}

func NewTopicMetaFromValues(description string, numPartitions uint32, replicationFactor uint32,
	lastOffset uint64, numPublishers uint64, numSubscribers uint64) *TopicData {

	data := make([]byte, uint64Len*3+uint32Len*2)
	binary.BigEndian.PutUint64(data[0:], lastOffset)
	binary.BigEndian.PutUint64(data[uint64Len:], numPublishers)
	binary.BigEndian.PutUint64(data[uint64Len*2:], numSubscribers)
	binary.BigEndian.PutUint32(data[uint64Len*3:], numPartitions)
	binary.BigEndian.PutUint32(data[uint64Len*3+uint32Len:], replicationFactor)
	data = append(data, description...)

	return &TopicData{data: data}
}

func (t TopicData) Data() []byte {
	return t.data
}

func (t TopicData) Size() int {
	return len(t.data)
}

func (t TopicData) Description() string {
	return string(t.Data()[uint64Len*3+uint32Len*2:])
}

func (t TopicData) NumPartitions() uint32 {
	return binary.BigEndian.Uint32(t.Data()[uint64Len*3 : uint64Len*3+uint32Len])
}

func (t TopicData) ReplicationFactor() uint32 {
	return binary.BigEndian.Uint32(t.Data()[uint64Len*3+uint32Len : uint64Len*3+uint32Len+uint32Len])
}

func (t TopicData) LastOffset() uint64 {
	return binary.BigEndian.Uint64(t.Data()[:uint64Len])
}

func (t TopicData) NumPublishers() uint64 {
	return binary.BigEndian.Uint64(t.Data()[uint64Len : uint64Len+uint64Len])
}

func (t TopicData) NumSubscribers() uint64 {
	return binary.BigEndian.Uint64(t.Data()[uint64Len*2 : uint64Len*2+uint64Len])
}
