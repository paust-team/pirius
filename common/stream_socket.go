package common

import (
	"errors"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"io"
	"sync"
)

type StreamSocket interface {
	Send(data *paustqproto.Data) error
	Recv() (*paustqproto.Data, error)
}

type StreamSocketContainer struct {
	sync.Mutex
	socket StreamSocket
	closed bool
}

func NewSocketContainer(socket StreamSocket) *StreamSocketContainer {
	return &StreamSocketContainer{sync.Mutex{}, socket, true}
}

func (sc *StreamSocketContainer) SetSocket(socket StreamSocket) {
	sc.socket = socket
}

func (sc *StreamSocketContainer) Open() {
	sc.Lock()
	defer sc.Unlock()
	sc.closed = false
}

// TODO:: need to chunk large marshaled proto message

func (sc *StreamSocketContainer) Write(msg *message.QMessage) error {

	if err := sc.socket.Send(&paustqproto.Data{MsgData: msg.Data}); err != nil {
		if err == io.EOF {
			return pqerror.SocketClosedError{}
		}
		return pqerror.SocketWriteError{ErrStr:err.Error()}
	}
	return nil
}

// This method is for reading chunked proto message from stream
func (sc *StreamSocketContainer) Read() (*message.QMessage, error) {

	receivedData, err := sc.socket.Recv()
	if err == io.EOF { // end stream
		return nil, pqerror.SocketClosedError{}
	}
	if err != nil {
		return nil, pqerror.SocketReadError{ErrStr:err.Error()}
	}
	return message.NewQMessage(receivedData.MsgData), nil
}

type Result struct {
	Msg *message.QMessage
	Err error
}

func (sc *StreamSocketContainer) ContinuousRead() <-chan Result {
	resultCh := make(chan Result)
	go func() {
		defer close(resultCh)
		for {
			msg, err := sc.Read()
			resultCh <- Result{msg, err}

			var e pqerror.SocketClosedError
			if errors.As(err, &e) {
				return
			}
		}
	}()

	return resultCh
}

func (sc *StreamSocketContainer) ContinuousWrite(writeCh <-chan *message.QMessage) <- chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		for msgToWrite := range writeCh {
			err := sc.Write(msgToWrite)
			if err != nil {
				errCh <- err

				var e pqerror.SocketClosedError
				if errors.As(err, &e) {
					return
				}
			}
		}
	}()

	return errCh
}

func (sc *StreamSocketContainer) Close() {
	sc.Lock()
	sc.closed = true
	sc.Unlock()
}

func (sc *StreamSocketContainer) IsClosed() bool {
	sc.Lock()
	defer sc.Unlock()
	return sc.closed
}
