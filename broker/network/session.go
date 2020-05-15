package network

import (
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/pqerror"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
	"time"
)

type SessionState struct {
	sync.Mutex
	stType SessionStateType
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
	state    SessionState
	sessType paustq_proto.SessionType
	topic    *internals.Topic
	rTimeout time.Duration
	wTimeout time.Duration
}

func NewSession() *Session {
	return &Session{
		state: SessionState{
			sync.Mutex{}, NONE,
		},
		topic: nil,
	}
}

func (s *Session) WithType(sessType paustq_proto.SessionType) *Session {
	s.sessType = sessType
	return s
}

func (s *Session) SetTopic(topic *internals.Topic) {
	s.topic = topic
}

func (s Session) Type() paustq_proto.SessionType {
	return s.sessType
}

func (s *Session) SetType(sessType paustq_proto.SessionType) {
	s.sessType = sessType
}

func (s Session) Topic() *internals.Topic {
	return s.topic
}

func (s Session) IsClosed() bool {
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

func (s *Session) State() SessionStateType {
	s.state.Lock()
	defer s.state.Unlock()
	return s.state.stType
}
