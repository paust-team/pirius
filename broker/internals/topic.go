package internals

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

type Topic struct {
	name                   string
	Size, NumPubs, NumSubs int64
}

func NewTopic(topicName string) *Topic {
	return &Topic{topicName, 0, 0, 0}
}

func (t Topic) Name() string {
	return t.name
}

func (t *Topic) LastOffset() uint64 {
	size := atomic.LoadInt64(&t.Size)
	if size == 0 {
		return 0
	}
	return uint64(size - 1)
}

type TopicValue struct {
	data    []byte
}

func NewTopicValue(data []byte) *TopicValue {
	return &TopicValue{data: data}
}

func NewTopicValueFromValues(topicMeta string, numPartitions uint32, replicationFactor uint32) *TopicValue {
	data := make([]byte, len(topicMeta)+int(unsafe.Sizeof(numPartitions))+int(unsafe.Sizeof(replicationFactor)))
	copy(data, topicMeta)
	binary.BigEndian.PutUint32(data[len(topicMeta):], numPartitions)
	binary.BigEndian.PutUint32(data[len(topicMeta)+int(unsafe.Sizeof(numPartitions)):], replicationFactor)

	return &TopicValue{data: data}
}

func (key TopicValue) Data() []byte {
	return key.data
}

func (key TopicValue) Size() int {
	return len(key.data)
}

func (key TopicValue) TopicMeta() string {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return string(key.Data()[:key.Size()-uint32Len*2])
}

func (key TopicValue) NumPartitions() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len*2 : key.Size()-uint32Len])
}

func (key TopicValue) ReplicationFactor() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len:])
}