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

var blockTime time.Duration = 500

type Socket struct {
	sync.Mutex
	conn       net.Conn
	rTimeout   int
	wTimeout   int
	closed     bool
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

func NewSocket(conn net.Conn, rTimeout int, wTimeout int) *Socket {
	ctx, cancel := context.WithCancel(context.Background())
	return &Socket{conn: conn, rTimeout: rTimeout, wTimeout: wTimeout, closed: false, ctx: ctx, cancelFunc: cancel, wg: sync.WaitGroup{}}
}

func (s *Socket) SetReadTimeout(rTimeout int) {
	s.rTimeout = rTimeout
}

func (s *Socket) SetWriteTimeout(wTimeout int) {
	s.wTimeout = wTimeout
}

func (s *Socket) Close() {
	s.Lock()
	defer s.Unlock()
	if !s.closed {
		s.closed = true
		s.cancelFunc()
		s.wg.Wait()
		s.conn.Close()
	}
}

func (s *Socket) IsClosed() bool {
	s.Lock()
	defer s.Unlock()
	return s.closed
}

func (s *Socket) continuousRead(ctx context.Context) (<-chan *message.QMessage, <-chan error) {
	msgStream := make(chan *message.QMessage)
	errCh := make(chan error)
	s.wg.Add(1)
	go func() {
		defer close(msgStream)
		defer close(errCh)
		defer s.wg.Done()

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
					} else if errors.Is(err, syscall.EPIPE) {
						errCh <- pqerror.BrokenPipeError{}
						return
					} else if errors.Is(err, syscall.ECONNRESET) {
						errCh <- pqerror.ConnResetError{}
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
						msgStream <- msg // message is sent, renew timeout
						processed = binary.Size(&Header{}) + len(msg.Data)
						data = data[processed:]
					}
				}
				runtime.Gosched()
				timeout = time.After(time.Millisecond * time.Duration(s.rTimeout))
			}
			runtime.Gosched()
		}
	}()
	return msgStream, errCh
}

func (s *Socket) continuousWrite(ctx context.Context, msgCh <-chan *message.QMessage) chan error {
	errCh := make(chan error)
	s.wg.Add(1)
	go func() {
		defer close(errCh)
		defer s.wg.Done()

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
							} else if errors.Is(err, syscall.EPIPE) {
								errCh <- pqerror.BrokenPipeError{}
								return
							} else if errors.Is(err, syscall.ECONNRESET) {
								errCh <- pqerror.ConnResetError{}
								return
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

	s.wg.Add(1)
	defer s.wg.Done()

	if _, err := s.conn.Write(data); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return pqerror.WriteTimeOutError{}
		} else if errors.Is(err, syscall.ECONNRESET) {
			return pqerror.ConnResetError{}
		} else if errors.Is(err, syscall.EPIPE) {
			return pqerror.BrokenPipeError{}
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

	var data []byte
	recvBuf := make([]byte, 4*1024)

	s.wg.Add(1)
	defer s.wg.Done()

	for !s.IsClosed() {
		err := s.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(s.rTimeout)))
		if err != nil {
			return nil, pqerror.SocketReadError{ErrStr: err.Error()}
		}

		n, err := s.conn.Read(recvBuf[0:])
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return nil, pqerror.ReadTimeOutError{}
			} else if errors.Is(err, syscall.EPIPE) {
				return nil, pqerror.BrokenPipeError{}
			} else if errors.Is(err, syscall.ECONNRESET) {
				return nil, pqerror.ConnResetError{}
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

	return nil, pqerror.SocketClosedError{}
}

func (s *Socket) ContinuousReadWrite() (<-chan *message.QMessage, chan<- *message.QMessage, <-chan error) {

	ctx, cancel := context.WithCancel(context.Background())

	readCh := make(chan *message.QMessage, 100)
	writeCh := make(chan *message.QMessage, 100)

	sockReadCh, readErrCh := s.continuousRead(ctx)
	socketWriteCh := make(chan *message.QMessage)

	writeErrCh := s.continuousWrite(ctx, socketWriteCh)

	errCh := pqerror.MergeErrors(readErrCh, writeErrCh)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-s.ctx.Done():
				cancel()
				return
			default:
			}
			runtime.Gosched()
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-sockReadCh:
				if !ok {
					return
				}
				readCh <- msg
			case msg, ok := <-writeCh:
				if ok && !s.IsClosed() {
					socketWriteCh <- msg
				}
			default:
			}
			runtime.Gosched()
		}
	}()

	// close channels when all continuous i/o routines are closed and context is done
	go func() {
		defer close(readCh)
		defer close(writeCh)
		defer close(socketWriteCh)
		wg.Wait()
	}()

	return readCh, writeCh, errCh
}
