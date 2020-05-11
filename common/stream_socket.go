package common

import (
	"errors"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"io"
	"math"
	"math/rand"
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

func (sc *StreamSocketContainer) Write(msg *message.QMessage, maxBufferSize uint32) error {

	totalChunkCount := uint32(math.Ceil(float64(len(msg.Data)) / float64(maxBufferSize)))
	chunkId := rand.Uint64()
	var currentChunkIdx uint32 = 0

	for ; currentChunkIdx < totalChunkCount; currentChunkIdx++ {
		start := currentChunkIdx * maxBufferSize
		end := (currentChunkIdx + 1) * maxBufferSize
		var data []byte
		if currentChunkIdx+1 < totalChunkCount {
			data = msg.Data[start:end]
		} else {
			data = msg.Data[start:]
		}

		header := &paustqproto.Header{ChunkId: chunkId, TotalChunkCount: totalChunkCount, CurrentChunkIdx: currentChunkIdx}
		body := &paustqproto.Body{Data: data}
		if err := sc.socket.Send(&paustqproto.Data{Header: header, Body: body}); err != nil {
			return pqerror.SocketWriteError{ErrStr:err.Error()}
		}
	}
	return nil
}

// This method is for reading chunked proto message from stream
func (sc *StreamSocketContainer) Read() (*message.QMessage, error) {
	var totalData []byte
	var chunkId uint64 = 0
	var prevChunkIdx uint32 = 0

	for {
		receivedData, err := sc.socket.Recv()
		if err == io.EOF { // end stream
			return nil, nil
		}
		if err != nil {
			return nil, pqerror.SocketReadError{ErrStr:err.Error()}
		}

		if chunkId == 0 {
			chunkId = receivedData.Header.ChunkId
		} else if chunkId != receivedData.Header.ChunkId {
			return nil, errors.New("cannot complete chunk: got different chunk id")
		}

		if receivedData.Header.CurrentChunkIdx != 0 && receivedData.Header.CurrentChunkIdx-1 != prevChunkIdx {
			return nil, errors.New("cannot complete chunk: got different chunk id")
		}
		prevChunkIdx = receivedData.Header.CurrentChunkIdx
		totalData = append(totalData, receivedData.Body.Data...)
		if receivedData.Header.CurrentChunkIdx >= receivedData.Header.TotalChunkCount-1 {
			break
		}
	}

	return message.NewQMessage(totalData), nil
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
		}
	}()

	return resultCh
}

func (sc *StreamSocketContainer) ContinuousWrite(writeCh <-chan *message.QMessage, errCh chan error) {
	go func() {
		for msgToWrite := range writeCh {
			err := sc.Write(msgToWrite, 1024)
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
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
