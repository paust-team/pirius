package network

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	"io"
	"net"
	"sync"
	"time"
)

type Socket struct {
	sync.Mutex
	conn     net.Conn
	rTimeout int
	wTimeout int
	closed   bool
}

func NewSocket(conn net.Conn, rTimeout int, wTimeout int) *Socket {
	return &Socket{conn: conn, rTimeout: rTimeout, wTimeout: wTimeout, closed: false}
}

func (s *Socket) SetReadTimeout(rTimeout int) {
	s.rTimeout = rTimeout
}

func (s *Socket) SetWriteTimeout(wTimeout int) {
	s.wTimeout = wTimeout
}

func (s *Socket) Close() {
	s.Lock()
	s.closed = true
	s.Unlock()
}

func (s *Socket) IsClosed() bool {
	s.Lock()
	defer s.Unlock()
	return s.closed
}

func (s *Socket) ContinuousRead(ctx context.Context) (<-chan *message.QMessage, <-chan error) {
	msgStream := make(chan *message.QMessage)
	errCh := make(chan error)
	var data []byte
	recvBuf := make([]byte, 4*1024)
	processed := 0

	go func() {
		defer close(msgStream)
		defer close(errCh)
		defer s.conn.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			err := s.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(s.rTimeout)))
			if err != nil {
				errCh <- pqerror.SocketReadError{ErrStr: err.Error()}
				return
			}

			n, err := s.conn.Read(recvBuf[0:])
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					if s.IsClosed() {
						errCh <- pqerror.SocketClosedError{}
						return
					}

					errCh <- pqerror.ReadTimeOutError{}
					return
				} else if err == io.EOF {
					errCh <- pqerror.SocketClosedError{}
					return
				} else {
					errCh <- pqerror.SocketReadError{ErrStr: err.Error()}
					return
				}
			}

			if n > 0 {
				data = append(data, recvBuf[:n]...)
				for {
					msg, err := Deserialize(data)

					if err != nil {
						if errors.As(err, &pqerror.NotEnoughBufferError{}) {
							break
						}
						errCh <- err
						return
					}

					msgStream <- msg
					processed = binary.Size(&Header{}) + len(msg.Data)
					data = data[processed:]
				}
			}
		}
	}()
	return msgStream, errCh
}

func (s *Socket) ContinuousWrite(ctx context.Context, msgCh <-chan *message.QMessage) chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		defer s.conn.Close()

		for msg := range msgCh {
			err := s.conn.SetWriteDeadline(time.Now().Add(time.Millisecond * time.Duration(s.wTimeout)))
			if err != nil {
				errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
				data, err := Serialize(msg)
				if err != nil {
					errCh <- err
					return
				}
				if _, err := s.conn.Write(data); err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						if s.IsClosed() {
							errCh <- pqerror.SocketClosedError{}
							return
						}
						errCh <- pqerror.WriteTimeOutError{}
						return
					} else if err == io.EOF {
						errCh <- pqerror.SocketClosedError{}
						return
					} else {
						errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
						return
					}
				}
			}
		}
	}()

	return errCh
}

func (s *Socket) Write(msg *message.QMessage) error {

	if s.IsClosed() {
		return pqerror.SocketClosedError{}
	}

	err := s.conn.SetWriteDeadline(time.Now().Add(time.Millisecond * time.Duration(s.wTimeout)))

	if err != nil {
		return err
	}

	data, err := Serialize(msg)
	if err != nil {
		return err
	}

	if _, err := s.conn.Write(data); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			if s.IsClosed() {
				s.conn.Close()
				return pqerror.SocketClosedError{}
			}
			return pqerror.WriteTimeOutError{}
		} else if err == io.EOF {
			return pqerror.SocketClosedError{}
		} else {
			return pqerror.SocketWriteError{ErrStr: err.Error()}
		}
	}

	return nil
}

func (s *Socket) Read() (*message.QMessage, error) {

	if s.IsClosed() {
		return nil, pqerror.SocketClosedError{}
	}

	var data []byte
	recvBuf := make([]byte, 4*1024)

	for {
		err := s.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(s.rTimeout)))
		if err != nil {
			return nil, pqerror.SocketReadError{ErrStr: err.Error()}
		}

		n, err := s.conn.Read(recvBuf[0:])
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if s.IsClosed() {
					s.conn.Close()
					return nil, pqerror.SocketClosedError{}
				}
				return nil, pqerror.ReadTimeOutError{}
			} else if err == io.EOF {
				return nil, pqerror.SocketClosedError{}
			} else {
				return nil, pqerror.SocketReadError{ErrStr: err.Error()}
			}
		}

		if n > 0 {
			data = append(data, recvBuf[:n]...)
			for {
				msg, err := Deserialize(data)
				if err != nil {
					if errors.As(err, &pqerror.NotEnoughBufferError{}) {
						break
					}
					return nil, err
				}
				return msg, nil
			}
		}
	}
}
