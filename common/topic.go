package common

import (
	"encoding/binary"
	"unsafe"
)

type FragmentOffsetMap map[uint32]uint64

type Topic struct {
	topicName       string
	fragmentOffsets FragmentOffsetMap
}

func NewTopicFromFragmentOffsets(topicName string, fragmentOffsets FragmentOffsetMap) *Topic {
	return &Topic{topicName: topicName, fragmentOffsets: fragmentOffsets}
}

func NewTopic(topicName string, fragmentIds []uint32) *Topic {
	fragmentsMap := FragmentOffsetMap{}
	for _, id := range fragmentIds {
		fragmentsMap[id] = 0
	}
	return &Topic{topicName: topicName, fragmentOffsets: fragmentsMap}
}

func (t Topic) TopicName() string {
	return t.topicName
}

func (t Topic) FragmentOffsets() FragmentOffsetMap {
	return t.fragmentOffsets
}

func (t Topic) FragmentIds() []uint32 {
	var fragmentIds []uint32
	for fid := range t.fragmentOffsets {
		fragmentIds = append(fragmentIds, fid)
	}
	return fragmentIds
}

func (t Topic) AddFragmentId(fragmentId uint32) {
	if _, exists := t.fragmentOffsets[fragmentId]; !exists {
		t.fragmentOffsets[fragmentId] = 0
	}
}

func (t Topic) AddFragmentOffset(fragmentId uint32, offset uint64) {
	if _, exists := t.fragmentOffsets[fragmentId]; !exists {
		t.fragmentOffsets[fragmentId] = offset
	}
}

type FrameForTopic struct {
	data []byte
}

var uint32Len = int(unsafe.Sizeof(uint32(0)))
var uint64Len = int(unsafe.Sizeof(uint64(0)))

func NewTopicData(data []byte) *FrameForTopic {
	return &FrameForTopic{data: data}
}

func NewTopicDataFromValues(description string, numFragments uint32, replicationFactor uint32, numPublishers uint64) *FrameForTopic {

	data := make([]byte, uint64Len+uint32Len*2)
	binary.BigEndian.PutUint64(data[0:], numPublishers)
	binary.BigEndian.PutUint32(data[uint64Len:], numFragments)
	binary.BigEndian.PutUint32(data[uint64Len+uint32Len:], replicationFactor)
	data = append(data, description...)

	return &FrameForTopic{data: data}
}

func (t FrameForTopic) Data() []byte {
	return t.data
}

func (t FrameForTopic) Size() int {
	return len(t.data)
}

func (t FrameForTopic) NumPublishers() uint64 {
	return binary.BigEndian.Uint64(t.Data()[:uint64Len])
}

func (t *FrameForTopic) SetNumPublishers(num uint64) {
	binary.BigEndian.PutUint64(t.data[0:], num)
}

func (t FrameForTopic) NumFragments() uint32 {
	return binary.BigEndian.Uint32(t.Data()[uint64Len : uint64Len+uint32Len])
}

func (t FrameForTopic) ReplicationFactor() uint32 {
	return binary.BigEndian.Uint32(t.Data()[uint64Len+uint32Len : uint64Len+uint32Len*2])
}

func (t FrameForTopic) Description() string {
	return string(t.Data()[uint64Len+uint32Len*2:])
}
