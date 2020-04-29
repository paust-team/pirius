package internals

import (
	"sync/atomic"
)

type Topic struct {
	name                   string
	Size, NumPubs, NumSubs int64
	External 				bool
	Endpoint				string
}

func NewTopic(topicName string) *Topic {
	return &Topic{topicName, 0, 0, 0, false, ""}
}

func NewExternalTopic(topicName, endpoint string) *Topic {
	return &Topic{topicName, 0, 0, 0, true, endpoint}
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
