package pqerror

import (
	"github.com/paust-team/paustq/message"
	"sync"
)

type PQError interface {
	Code() PQCode
	Error() string
}

type IsSessionCloseable interface {
	IsSessionCloseable()
}

// If error is client visible, error message gonna be sent to related client
type IsClientVisible interface {
	IsClientVisible()
}

// If error is broadcastable, all of the clients get error message
type IsBrokerStoppable interface {
	IsBrokerStoppable()
}

type ISBroadcastable interface {
	IsBroadcastable()
}

func NewErrorAckMsg(code PQCode, hint string) *message.QMessage {
	var ackMsg *message.QMessage
	if code == ErrInternal {
		ackMsg, _ = message.NewQMessageFromMsg(message.NewAckMsg(uint32(code), "broker internal error"))
	} else {
		ackMsg, _ = message.NewQMessageFromMsg(message.NewAckMsg(uint32(code), hint))
	}
	return ackMsg
}

type ErrBroadcaster struct {
	sync.Mutex
	BroadcastChs []chan error
}

func (b *ErrBroadcaster) AddChannel(ch chan error) {
	b.Lock()
	b.BroadcastChs = append(b.BroadcastChs, ch)
	b.Unlock()
}

func (b *ErrBroadcaster) RemoveChannel(ch chan error) {
	for i, broadcastCh := range b.BroadcastChs {
		if broadcastCh == ch {
			b.Lock()
			b.BroadcastChs = append(b.BroadcastChs[:i], b.BroadcastChs[i+1:]...)
			b.Unlock()
		}
	}
}

func MergeErrors(errChannels ...<-chan error) <-chan error {
	var wg sync.WaitGroup

	out := make(chan error, len(errChannels))
	output := func(c <-chan error) {
		defer wg.Done()
		for n := range c {
			out <- n
		}
	}

	wg.Add(len(errChannels))
	for _, c := range errChannels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
