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
	"time"
)

var blockTime time.Duration = 500

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

	go func() {
		defer close(msgStream)
		defer close(errCh)

		var data []byte
		recvBuf := make([]byte, 4*1024)
		processed := 0

		timeout := time.After(time.Millisecond * time.Duration(s.rTimeout))
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout:
				errCh <- pqerror.ReadTimeOutError{}
				return
			default:
				err := s.conn.SetReadDeadline(time.Now().Add(time.Millisecond * blockTime))
				if err != nil {
					errCh <- pqerror.SocketReadError{ErrStr: err.Error()}
					return
				}

				n, err := s.conn.Read(recvBuf[0:])
				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						runtime.Gosched()
						continue
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

						msgStream <- msg // message is sent, renew timeout
						time.After(time.Millisecond * time.Duration(s.rTimeout))
						processed = binary.Size(&Header{}) + len(msg.Data)
						data = data[processed:]
					}
				}

				runtime.Gosched()
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
		defer s.conn.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}

				timeout := time.After(time.Millisecond * time.Duration(s.wTimeout))
			writeLoop:
				for {
					select {
					case <-ctx.Done():
						return
					case <-timeout:
						errCh <- pqerror.WriteTimeOutError{}
						return
					default:
						err := s.conn.SetWriteDeadline(time.Now().Add(time.Millisecond * blockTime))
						if err != nil {
							errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
							return
						}
						data, err := Serialize(msg)
						if err != nil {
							errCh <- err
							return
						}
						if _, err := s.conn.Write(data); err != nil {
							if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
								runtime.Gosched()
								continue
							} else if err == io.EOF {
								errCh <- pqerror.SocketClosedError{}
								return
							} else {
								errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
								return
							}
						} else {
							break writeLoop
						}
					}
				}
			}
			runtime.Gosched()
		}
	}()

	return errCh
}

// don't call this while continuous writing
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

// don't call this while continuous reading
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
			msg, err := Deserialize(data)
			if err != nil {
				if errors.As(err, &pqerror.NotEnoughBufferError{}) {
					continue
				}
				return nil, err
			}
			return msg, nil
		}
		runtime.Gosched()
	}
}
