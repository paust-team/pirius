package common

import (
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"io"
)

type FlowStreamService interface {
	Send(*paustqproto.StreamData) error
	Recv() (*paustqproto.StreamData, error)
}

type StreamReaderWriter struct {
	Stream FlowStreamService
}

func NewStreamReaderWriter(stream FlowStreamService) *StreamReaderWriter {
	return &StreamReaderWriter{Stream: stream}
}

// TODO:: need to chunk large marshaled proto message
func (rw *StreamReaderWriter) SendMsg(msg *message.QMessage) error {

	header := &paustqproto.StreamHeader{TotalChunkCount: 1, CurrentChunkIdx: 0}
	body := &paustqproto.StreamBody{Data: msg.Data}

	return rw.Stream.Send(&paustqproto.StreamData{Magic: -1, Header: header, Body: body})
}

// This method is for reading chunked proto message from stream
func (rw *StreamReaderWriter) RecvMsg() (*message.QMessage, error) {
	var totalData []byte
	for {
		receivedData, err := rw.Stream.Recv()
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
