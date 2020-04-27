package internals

import "sync"

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

func (t *Topic) WaitPublish() {
	t.published.L.Lock()
	defer t.published.L.Unlock()
	t.published.Wait()
}

func (t *Topic) NotifyPublished() {
	t.published.Broadcast()
}
