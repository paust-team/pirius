package common

type FragmentOffsetMap map[uint32]uint64

type Topic struct {
	topicName       string
	fragmentOffsets FragmentOffsetMap
	maxBatchSize    uint32
	flushInterval   uint32
}

func NewTopicFromFragmentOffsets(topicName string, fragmentOffsets FragmentOffsetMap, maxBatchSize, flushInterval uint32) *Topic {
	return &Topic{topicName: topicName, fragmentOffsets: fragmentOffsets, maxBatchSize: maxBatchSize, flushInterval: flushInterval}
}

func NewTopic(topicName string, fragmentIds []uint32, maxBatchSize, flushInterval uint32) *Topic {
	fragmentsMap := FragmentOffsetMap{}
	for _, id := range fragmentIds {
		fragmentsMap[id] = 0
	}
	return &Topic{topicName: topicName, fragmentOffsets: fragmentsMap, maxBatchSize: maxBatchSize, flushInterval: flushInterval}
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

func (t Topic) MaxBatchSize() uint32 {
	return t.maxBatchSize
}

func (t Topic) FlushInterval() uint32 {
	return t.flushInterval
}
