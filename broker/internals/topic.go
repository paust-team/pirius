package internals

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

type Topic struct {
	name             string
	meta             *TopicMeta
	NumPubs, NumSubs int64
	lastOffset       uint64
}

func NewTopic(topicName string, topicMeta *TopicMeta) *Topic {
	return &Topic{topicName, topicMeta, 0, 0, topicMeta.LastOffset()}
}

func (t Topic) Name() string {
	return t.name
}

func (t *Topic) LastOffset() uint64 {
	return atomic.LoadUint64(&t.lastOffset)
}

func (t *Topic) IncreaseLastOffset() uint64 {
	lastOffset := atomic.AddUint64(&t.lastOffset, 1)
	t.meta.SetLastOffset(lastOffset)
	return lastOffset
}

type TopicMeta struct {
	data []byte
}

func NewTopicMeta(data []byte) *TopicMeta {
	return &TopicMeta{data: data}
}

func NewTopicMetaFromValues(description string, numPartitions uint32, replicationFactor uint32, lastOffset uint64) *TopicMeta {
	dSize := len(description)
	nSize := int(unsafe.Sizeof(numPartitions))
	rSize := int(unsafe.Sizeof(replicationFactor))
	lSize := int(unsafe.Sizeof(lastOffset))

	data := make([]byte, dSize+nSize+rSize+lSize)
	copy(data, description)
	binary.BigEndian.PutUint32(data[dSize:], numPartitions)
	binary.BigEndian.PutUint32(data[dSize+nSize:], replicationFactor)
	binary.BigEndian.PutUint64(data[dSize+nSize+rSize:], lastOffset)

	return &TopicMeta{data: data}
}

func (key TopicMeta) Data() []byte {
	return key.data
}

func (key TopicMeta) Size() int {
	return len(key.data)
}

func (key TopicMeta) Description() string {
	descriptionLen := key.Size() - int(unsafe.Sizeof(uint32(0)))*2 - int(unsafe.Sizeof(uint64(0)))
	return string(key.Data()[:descriptionLen])
}

func (key TopicMeta) NumPartitions() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	uint64Len := int(unsafe.Sizeof(uint64(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len*2-uint64Len : key.Size()-uint32Len-uint64Len])
}

func (key TopicMeta) ReplicationFactor() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	uint64Len := int(unsafe.Sizeof(uint64(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len-uint64Len : key.Size()-uint64Len])
}

func (key TopicMeta) LastOffset() uint64 {
	uint64Len := int(unsafe.Sizeof(uint64(0)))
	return binary.BigEndian.Uint64(key.Data()[key.Size()-uint64Len:])
}

func (key *TopicMeta) SetLastOffset(lastOffset uint64) {
	uint64Len := int(unsafe.Sizeof(uint64(0)))
	binary.BigEndian.PutUint64(key.Data()[key.Size()-uint64Len:], lastOffset)
}
