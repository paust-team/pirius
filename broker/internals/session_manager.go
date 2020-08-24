package internals

import (
	"github.com/paust-team/shapleq/message"
	"sync"
)

type SessionManager struct {
	sync.Mutex
	sessions []*Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{Mutex: sync.Mutex{}}
}

func (s *SessionManager) AddSession(session *Session) {
	s.Lock()
	defer s.Unlock()
	s.sessions = append(s.sessions, session)
}

func (s *SessionManager) RemoveSession(session *Session) {
	s.Lock()
	defer s.Unlock()
	for i, ss := range s.sessions {
		if ss == session {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			break
		}
	}
}

func (s *SessionManager) BroadcastMsg(msg *message.QMessage) {
	// TODO::
	//s.Lock()
	//defer s.Unlock()
	//
	//for _, session := range s.sessions {
	//	session.Write(msg)
	//}
}
