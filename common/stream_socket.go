package common

import (
	"github.com/paust-team/paustq/message"
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

	header := &paustqproto.Header{TotalChunkCount: 1, CurrentChunkIdx: 0}
	body := &paustqproto.Body{Data: msg.Data}

	return sc.socket.Send(&paustqproto.Data{Magic: -1, Header: header, Body: body})
}

// This method is for reading chunked proto message from stream
func (sc *StreamSocketContainer) Read() (*message.QMessage, error) {
	var totalData []byte
	for {
		receivedData, err := sc.socket.Recv()
		if err == io.EOF { // end stream
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		totalData = append(totalData, receivedData.Body.Data...)
		if receivedData.Header.CurrentChunkIdx >= receivedData.Header.TotalChunkCount - 1 {
			break
		}
	}

	return message.NewQMessage(totalData), nil
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
