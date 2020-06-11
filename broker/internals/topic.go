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

type TopicMeta struct {
	data    []byte
}

func NewTopicMeta(data []byte) *TopicMeta {
	return &TopicMeta{data: data}
}

func NewTopicMetaFromValues(description string, numPartitions uint32, replicationFactor uint32) *TopicMeta {
	data := make([]byte, len(description)+int(unsafe.Sizeof(numPartitions))+int(unsafe.Sizeof(replicationFactor)))
	copy(data, description)
	binary.BigEndian.PutUint32(data[len(description):], numPartitions)
	binary.BigEndian.PutUint32(data[len(description)+int(unsafe.Sizeof(numPartitions)):], replicationFactor)

	return &TopicMeta{data: data}
}

func (key TopicMeta) Data() []byte {
	return key.data
}

func (key TopicMeta) Size() int {
	return len(key.data)
}

func (key TopicMeta) TopicMeta() string {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return string(key.Data()[:key.Size()-uint32Len*2])
}

func (key TopicMeta) NumPartitions() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len*2 : key.Size()-uint32Len])
}

func (key TopicMeta) ReplicationFactor() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len:])
}