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
	rTimeout int64
	wTimeout int64
}

func NewSocket(conn net.Conn, rTimeout int64, wTimeout int64) *Socket {
	return &Socket{conn: conn, rTimeout: rTimeout, wTimeout: wTimeout}
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
					if errors.As(err, &pqerror.NotEnoughBufferError{}) {
						break
					}

					if err != nil {
						if errors.As(err, &pqerror.InvalidChecksumError{}) {
							errCh <- err
							return
						}
					}

					msgStream <- msg
					processed = binary.Size(&Header{}) + len(msg.Data)
					data = data[processed:]
					processed = 0
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
		for {
			err := s.conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(s.wTimeout)))
			if err != nil {
				errCh <- pqerror.SocketWriteError{ErrStr: err.Error()}
				return
			}

			select {
			case <-ctx.Done():
				return
			case msg := <-msgCh:
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
