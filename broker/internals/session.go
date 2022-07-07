package internals

import (
	"context"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"net"
	"sync"
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
	state    *SessionState
	sessType shapleq_proto.SessionType
	topics   []*common.Topic
	rTimeout uint
	wTimeout uint
}

func NewSession(conn net.Conn, timeout int) *Session {
	return &Session{
		sock: network.NewSocket(conn, timeout, timeout),
		state: &SessionState{
			sync.RWMutex{}, NONE,
		},
		topics: []*common.Topic{},
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

func (s *Session) Type() shapleq_proto.SessionType {
	return s.sessType
}

func (s *Session) SetType(sessType shapleq_proto.SessionType) {
	s.sessType = sessType
}

func (s *Session) Topics() []*common.Topic {
	return s.topics
}

func (s *Session) SetTopics(topics []*common.Topic) {
	s.topics = topics
}

func (s *Session) AddTopic(topic *shapleq_proto.Topic) {
	startOffsets := common.FragmentOffsetMap{}
	for _, offset := range topic.Offsets {
		startOffsets[offset.FragmentId] = offset.StartOffset
	}

	topicFragments := common.NewTopicFromFragmentOffsets(topic.TopicName, startOffsets, topic.MaxBatchSize, topic.FlushInterval)
	s.topics = append(s.topics, topicFragments)
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
