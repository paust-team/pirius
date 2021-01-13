package network

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	"io"
	"net"
	"runtime"
	"sync"
	"syscall"
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
	s.conn.Close()
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
				} else if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
					errCh <- pqerror.SocketClosedError{}
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
			runtime.Gosched()
		}
	}()
	return msgStream, errCh
}

func (s *Socket) ContinuousWrite(ctx context.Context, msgCh <-chan *message.QMessage) chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		waitUntilFinished := false
		for msg := range msgCh {
			select {
			case <-ctx.Done():
				waitUntilFinished = true
			default:
				if !waitUntilFinished {
					err := s.conn.SetWriteDeadline(time.Now().Add(time.Millisecond * time.Duration(s.wTimeout)))
					if err != nil {
						errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
						waitUntilFinished = true
					}
					data, err := Serialize(msg)
					if err != nil {
						errCh <- err
						waitUntilFinished = true
					}
					if _, err := s.conn.Write(data); err != nil {
						if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
							if s.IsClosed() {
								errCh <- pqerror.SocketClosedError{}
								waitUntilFinished = true
							}
							errCh <- pqerror.WriteTimeOutError{}
							waitUntilFinished = true
						} else if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
							errCh <- pqerror.SocketClosedError{}
							waitUntilFinished = true
						} else if err == io.EOF {
							errCh <- pqerror.SocketClosedError{}
							waitUntilFinished = true
						} else {
							errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
							waitUntilFinished = true
						}
					}
				}
			}
			runtime.Gosched()
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
				return pqerror.SocketClosedError{}
			}
			return pqerror.WriteTimeOutError{}
		} else if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
			return pqerror.SocketClosedError{}
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
					return nil, pqerror.SocketClosedError{}
				}
				return nil, pqerror.ReadTimeOutError{}
			} else if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
				return nil, pqerror.SocketClosedError{}
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
		runtime.Gosched()
	}
}
