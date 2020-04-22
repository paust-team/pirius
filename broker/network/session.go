package network

import (
	"errors"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
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

var stateTransition = map[SessionStateType][]SessionStateType {
	NONE: {NONE, READY},
	READY: {NONE, READY, ON_PUBLISH, ON_SUBSCRIBE},
	ON_PUBLISH: {NONE, READY},
	ON_SUBSCRIBE: {NONE, READY},
}

type Session struct {
	sock *common.StreamSocketContainer
	state SessionState
	sessType  paustq_proto.SessionType
	topic     *internals.Topic
	rTimeout  time.Duration
	wTimeout  time.Duration
}

func NewSession(socket *common.StreamSocketContainer) *Session{
	return &Session{
		sock: socket,
		state: SessionState{
			sync.Mutex{}, NONE,
		},
	}
}

func (s *Session) WithType(sessType paustq_proto.SessionType) *Session {
	s.sessType = sessType
	return s
}

func (s *Session) WithTopic(topic *internals.Topic) *Session {
	s.topic = topic
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

func (s *Session) Open() {
	s.sock.Open()
}

func (s *Session) Close() {
	_ = s.SetState(NONE)
	s.sock.Close()
}

func (s Session) IsClosed() bool {
	return s.State() == NONE
}

func (s *Session) Read() (*message.QMessage, error) {
	return s.sock.Read()
}

func (s *Session) Write(msg *message.QMessage) error {
	return s.sock.Write(msg)
}



/*
func (s *Session) Read(ctx context.Context) (<-chan any.Any, <-chan error){
	anyStream := make(chan any.Any)
	errCh := make(chan error)
	var data []byte
	recvBuf := make([]byte, 4 * 1024)
	processed := 0

	func() {
		defer close(anyStream)
		defer close(errCh)
		for {
			err := s.conn.SetReadDeadline(time.Now().Add(s.rTimeout))
			if err != nil {
				errCh <- err
				return
			}
			n, err := s.conn.Read(recvBuf)
			if err != nil {
				errCh <- err
				return
			}
			// To Do: handle checksum error
			if msg, err := message.Deserialize(data[:processed]); err != nil {
				pb := any.Any{}
				err = proto.Unmarshal(msg, &pb)
				if err != nil {
					errCh <- err
					return
				}
				anyStream <- pb
				processed = int(unsafe.Sizeof(&Header{})) + len(msg)
				data = data[processed:]
				processed = 0
			}

			select {
			case <- ctx.Done():
				return
			default:
				data = append(data, recvBuf[:n]...)
				processed += n
				recvBuf = recvBuf[:0]
			}
		}
	}()

	return anyStream, errCh
}
func (s Session) Write(ctx context.Context, data []byte) error {
	return nil
}
*/

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
		return errors.New("invalid state transition")
	}
}

func (s *Session) State() SessionStateType {
	s.state.Lock()
	defer s.state.Unlock()
	return s.state.stType
}








