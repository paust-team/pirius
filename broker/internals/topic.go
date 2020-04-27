package internals

import (
	"sync"
	"sync/atomic"
)

type Topic struct {
	name                   string
	Size, NumPubs, NumSubs uint64
	published              *sync.Cond
}

func NewTopic(topicName string) *Topic {
	return &Topic{topicName, 0, 0, 0, sync.NewCond(&sync.Mutex{})}
}

func (t Topic) Name() string {
	return t.name
}

func (t *Topic) LastOffset() uint64 {
	size := atomic.LoadUint64(&t.Size)
	if size == 0 {
		return 0
	}
	return size - 1
}
