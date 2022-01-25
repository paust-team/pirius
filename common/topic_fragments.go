package common

type FragmentOffsetMap map[uint32]uint64

type TopicFragments struct {
	topicName       string
	fragmentOffsets FragmentOffsetMap
}

func NewTopicFragmentsWithOffset(topicName string, fragmentOffsets FragmentOffsetMap) *TopicFragments {
	return &TopicFragments{topicName: topicName, fragmentOffsets: fragmentOffsets}
}

func NewTopicFragments(topicName string, fragmentIds []uint32) *TopicFragments {
	fragmentsMap := FragmentOffsetMap{}
	for _, id := range fragmentIds {
		fragmentsMap[id] = 0
	}
	return &TopicFragments{topicName: topicName, fragmentOffsets: fragmentsMap}
}

func (t TopicFragments) Topic() string {
	return t.topicName
}

func (t TopicFragments) FragmentOffsets() FragmentOffsetMap {
	return t.fragmentOffsets
}

func (t TopicFragments) FragmentIds() []uint32 {
	var fragmentIds []uint32
	for fid := range t.fragmentOffsets {
		fragmentIds = append(fragmentIds, fid)
	}
	return fragmentIds
}

func (t TopicFragments) AddFragmentId(fragmentId uint32) {
	if _, exists := t.fragmentOffsets[fragmentId]; !exists {
		t.fragmentOffsets[fragmentId] = 0
	}
}

func (t TopicFragments) AddFragmentOffset(fragmentId uint32, offset uint64) {
	if _, exists := t.fragmentOffsets[fragmentId]; !exists {
		t.fragmentOffsets[fragmentId] = offset
	}
}
