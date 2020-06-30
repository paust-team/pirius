package network

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	"io"
	"net"
	"sync"
	"time"
)

type Socket struct {
	sync.Mutex
	conn     net.Conn
	rTimeout uint
	wTimeout uint
}

func NewSocket(conn net.Conn, rTimeout uint, wTimeout uint) *Socket {
	return &Socket{conn: conn, rTimeout: rTimeout, wTimeout: wTimeout}
}

func (s *Socket) SetReadTimeout(rTimeout uint) {
	s.rTimeout = rTimeout
}

func (s *Socket) SetWriteTimeout(wTimeout uint) {
	s.wTimeout = wTimeout
}

func (s *Socket) Close() {
	s.conn.Close()
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

			err := s.conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.rTimeout)))
			if err != nil {
				errCh <- pqerror.SocketReadError{ErrStr: err.Error()}
				return
			}

			n, err := s.conn.Read(recvBuf[0:])
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					errCh <- pqerror.ReadTimeOutError{}
					return
				} else if err != io.EOF {
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
		for msg := range msgCh {
			err := s.conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(s.wTimeout)))
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
						errCh <- pqerror.WriteTimeOutError{}
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
	err := s.conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(s.wTimeout)))
	if err != nil {
		return err
	}

	data, err := Serialize(msg)
	if err != nil {
		return err
	}

	if _, err := s.conn.Write(data); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return pqerror.WriteTimeOutError{}
		} else {
			return pqerror.SocketWriteError{ErrStr: err.Error()}
		}
	}

	return nil
}

func (s *Socket) Read() (*message.QMessage, error) {

	var data []byte
	recvBuf := make([]byte, 4*1024)

	for {
		err := s.conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.rTimeout)))
		if err != nil {
			return nil, pqerror.SocketReadError{ErrStr: err.Error()}
		}

		n, err := s.conn.Read(recvBuf[0:])
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return nil, pqerror.ReadTimeOutError{}
			} else if err != io.EOF {
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
