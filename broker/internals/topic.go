package internals

import "sync"

type Topic struct {
	name                   string
	Size, NumPubs, NumSubs uint64
	published              *sync.Cond
}

func NewTopic() *Topic{
	return &Topic{"paustq", 0, 0,0, sync.NewCond(&sync.Mutex{})}
}

func (t Topic) Name() string {
	return "paustq"
}

func (t *Topic) WaitPublish() {
	t.published.L.Lock()
	defer t.published.L.Unlock()
	t.published.Wait()
}

func (t *Topic) NotifyPublished() {
	t.published.Broadcast()
}

