package internals

import (
	"github.com/paust-team/paustq/message"
	"sync"
)

type Broadcaster struct {
	sync.Mutex
	BroadcastChs []chan *message.QMessage
}

func (b *Broadcaster) AddChannel(ch chan *message.QMessage) {
	b.Lock()
	b.BroadcastChs = append(b.BroadcastChs, ch)
	b.Unlock()
}

func (b *Broadcaster) RemoveChannel(ch chan *message.QMessage) {
	for i, broadcastCh := range b.BroadcastChs {
		if broadcastCh == ch {
			b.Lock()
			b.BroadcastChs = append(b.BroadcastChs[:i], b.BroadcastChs[i+1:]...)
			b.Unlock()
		}
	}
}

func (b *Broadcaster) Broadcast(msg *message.QMessage) {
	b.Lock()
	defer b.Unlock()
	for _, broadcastCh := range b.BroadcastChs {
		broadcastCh <- msg
	}
}
