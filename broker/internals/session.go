package internals

import (
	"context"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"net"
	"sync"
	"sync/atomic"
)

type EventStream struct {
	Session       *Session
	ReadMsgCh     <-chan *message.QMessage
	WriteMsgCh    chan<- *message.QMessage
	Ctx           context.Context
	CancelSession context.CancelFunc
}

type SessionState struct {
	sync.RWMutex
	stType SessionStateType
}

type TransactionalError struct {
	pqerror.PQError
	Session       *Session
	CancelSession context.CancelFunc
}

type SessionError struct {
	pqerror.PQError
	Session       *Session
	CancelSession context.CancelFunc
}

type SessionStateType uint8

const (
	NONE SessionStateType = iota
	READY
	ON_PUBLISH
	ON_SUBSCRIBE
)

func (st SessionStateType) String() string {
	switch st {
	case NONE:
		return "NONE"
	case READY:
		return "READY"
	case ON_PUBLISH:
		return "ON_PUBLISH"
	case ON_SUBSCRIBE:
		return "ON_SUBSCRIBE"
	default:
		return ""
	}
}

var stateTransition = map[SessionStateType][]SessionStateType{
	NONE:         {NONE, READY},
	READY:        {NONE, READY, ON_PUBLISH, ON_SUBSCRIBE},
	ON_PUBLISH:   {NONE, READY},
	ON_SUBSCRIBE: {NONE, READY},
}

type Session struct {
	sock     *network.Socket
	state    *SessionState
	sessType shapleq_proto.SessionType
	topic    *Topic
	rTimeout uint
	wTimeout uint
}

func NewSession(conn net.Conn, timeout int) *Session {
	return &Session{
		sock: network.NewSocket(conn, timeout, timeout),
		state: &SessionState{
			sync.RWMutex{}, NONE,
		},
		topic: nil,
	}
}

func (s *Session) WithType(sessType shapleq_proto.SessionType) *Session {
	s.sessType = sessType
	return s
}

func (s *Session) WithReadTimeout(rTimeout int) *Session {
	s.sock.SetReadTimeout(rTimeout)
	return s
}

func (s *Session) WithWriteTimeout(wTimeout int) *Session {
	s.sock.SetWriteTimeout(wTimeout)
	return s
}

func (s *Session) SetTopic(topic *Topic) {
	s.topic = topic
}

func (s *Session) Type() shapleq_proto.SessionType {
	return s.sessType
}

func (s *Session) SetType(sessType shapleq_proto.SessionType) {
	s.sessType = sessType
}

func (s *Session) Topic() *Topic {
	return s.topic
}

func (s *Session) State() SessionStateType {
	s.state.RLock()
	defer s.state.RUnlock()
	return s.state.stType
}

func (s *Session) Open() {
	s.SetState(READY)
}

func (s *Session) Close() {
	s.SetState(NONE)
	s.sock.Close()
	switch s.Type() {
	case shapleq_proto.SessionType_PUBLISHER:
		if atomic.LoadInt64(&s.Topic().NumPubs) > 0 {
			atomic.AddInt64(&s.Topic().NumPubs, -1)
		}
	case shapleq_proto.SessionType_SUBSCRIBER:
		if atomic.LoadInt64(&s.Topic().NumSubs) > 0 {
			atomic.AddInt64(&s.Topic().NumSubs, -1)
		}
	}
}
func (s *Session) ContinuousReadWrite() (<-chan *message.QMessage, chan<- *message.QMessage, <-chan error) {
	return s.sock.ContinuousReadWrite()
}

func (s *Session) IsClosed() bool {
	s.state.RLock()
	defer s.state.RUnlock()
	return s.state.stType == NONE
}

func (s *Session) SetState(nextState SessionStateType) error {
	contains := func(states []SessionStateType, state SessionStateType) bool {
		for _, s := range states {
			if s == state {
				return true
			}
		}
		return false
	}

	s.state.Lock()
	defer s.state.Unlock()
	if contains(stateTransition[s.state.stType], nextState) {
		s.state.stType = nextState
		return nil
	} else {
		return pqerror.StateTransitionError{PrevState: s.state.stType.String(), NextState: nextState.String()}
	}
}
