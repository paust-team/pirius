package common

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
