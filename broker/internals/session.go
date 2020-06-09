package internals

import (
	"context"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/network"
	"github.com/paust-team/paustq/pqerror"
	paustq_proto "github.com/paust-team/paustq/proto"
	"net"
	"sync"
	"sync/atomic"
)

type EventStream struct {
	Session       *Session
	MsgCh         <-chan *message.QMessage
	Ctx           context.Context
	CancelSession context.CancelFunc
}

type SessionState struct {
	sync.RWMutex
	stType SessionStateType
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
	state    SessionState
	sessType paustq_proto.SessionType
	topic    *Topic
	rTimeout uint
	wTimeout uint
}

const (
	// default time outs (second)
	DEFAULT_READ_TIMEOUT  uint = 5
	DEFAULT_WRITE_TIMEOUT uint = 5
)

func NewSession(conn net.Conn) *Session {
	return &Session{
		sock: network.NewSocket(conn, DEFAULT_READ_TIMEOUT, DEFAULT_WRITE_TIMEOUT),
		state: SessionState{
			sync.RWMutex{}, NONE,
		},
		topic: nil,
	}
}

func (s *Session) WithType(sessType paustq_proto.SessionType) *Session {
	s.sessType = sessType
	return s
}

func (s *Session) WithReadTimeout(rTimeout uint) *Session {
	s.sock.SetReadTimeout(rTimeout)
	return s
}

func (s *Session) WithWriteTimeout(wTimeout uint) *Session {
	s.sock.SetWriteTimeout(wTimeout)
	return s
}

func (s *Session) SetTopic(topic *Topic) {
	s.topic = topic
}

func (s Session) Type() paustq_proto.SessionType {
	return s.sessType
}

func (s *Session) SetType(sessType paustq_proto.SessionType) {
	s.sessType = sessType
}

func (s Session) Topic() *Topic {
	return s.topic
}

func (s *Session) State() SessionStateType {
	s.state.Lock()
	defer s.state.Unlock()
	return s.state.stType
}

func (s *Session) Open() {
	s.SetState(READY)
}

func (s *Session) Close() {
	s.SetState(NONE)
	s.sock.Close()

	switch s.Type() {
	case paustq_proto.SessionType_PUBLISHER:
		if atomic.LoadInt64(&s.Topic().NumPubs) > 0 {
			atomic.AddInt64(&s.Topic().NumPubs, -1)
		}
	case paustq_proto.SessionType_SUBSCRIBER:
		if atomic.LoadInt64(&s.Topic().NumSubs) > 0 {
			atomic.AddInt64(&s.Topic().NumSubs, -1)
		}
	}
}

func (s Session) IsClosed() bool {
	s.state.RLock()
	defer s.state.RUnlock()
	return s.State() == NONE
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

func (s *Session) ContinuousRead(ctx context.Context) (<-chan *message.QMessage, <-chan error, error) {
	if s.IsClosed() {
		return nil, nil, pqerror.SocketClosedError{}
	}

	msgCh, errCh := s.sock.ContinuousRead(ctx)
	return msgCh, errCh, nil
}

func (s *Session) ContinuousWrite(ctx context.Context, msgCh <-chan *message.QMessage) (chan error, error) {
	if s.IsClosed() {
		return nil, pqerror.SocketClosedError{}
	}

	errCh := s.sock.ContinuousWrite(ctx, msgCh)
	return errCh, nil
}

func (s *Session) Write(msg *message.QMessage) error {
	if s.IsClosed() {
		return pqerror.SocketClosedError{}
	}
	return s.sock.Write(msg)
}
