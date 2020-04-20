package client

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
)

type ReceivedData struct {
	Error error
	Msg  *message.QMessage
}

type StreamClient struct {
	context 			context.Context
	streamClient 		paustqproto.StreamService_FlowStreamClient
	streamReaderWriter 	*common.StreamReaderWriter
	SessionType 		paustqproto.SessionType
	conn 				*grpc.ClientConn
	ServerUrl			string
	Connected 			bool
}

func NewStreamClient(context context.Context, serverUrl string, sessionType paustqproto.SessionType) *StreamClient {
	return &StreamClient{context: context, SessionType: sessionType, ServerUrl: serverUrl}
}

func (client *StreamClient) ReceiveToChan(receiveCh chan <- ReceivedData) {

	msg, err := client.Receive()
	receiveCh <- ReceivedData{err, msg}
}

func (client *StreamClient) Receive() (*message.QMessage, error) {
	return client.streamReaderWriter.RecvMsg()
}

func (client *StreamClient) Send(msg *message.QMessage) error {
	return client.streamReaderWriter.SendMsg(msg)
}

func (client *StreamClient) ConnectWithTopic(topicName string) error {

	if client.Connected {
		return errors.New("already connected")
	}

	conn, err := grpc.Dial(client.ServerUrl, grpc.WithInsecure())
	if err != nil {
		return err
	}
	client.conn = conn
	client.Connected = true

	ctx, cancel := context.WithCancel(client.context)
	stub := paustqproto.NewStreamServiceClient(conn)
	stream, err := stub.FlowStream(ctx)
	if err != nil {
		conn.Close()
		cancel()
		return err
	}

	streamReaderWriter := common.NewStreamReaderWriter(stream)
	reqMsg, err := message.NewQMessageWithMsg(message.NewConnectRequestMsg(client.SessionType, topicName))
	if err != nil {
		conn.Close()
		cancel()
		return err
	}

	if err:= streamReaderWriter.SendMsg(reqMsg); err != nil {
		conn.Close()
		cancel()
		return err
	}

	respMsg, err := streamReaderWriter.RecvMsg()
	if err != nil {
		conn.Close()
		cancel()
		return err
	}

	connectResponseMsg := &paustqproto.ConnectResponse{}
	if err := respMsg.UnpackTo(connectResponseMsg); err != nil {
		conn.Close()
		cancel()
		return err
	}

	client.conn = conn
	client.Connected = true
	client.streamClient = stream
	client.streamReaderWriter = streamReaderWriter

	return nil
}

func (client *StreamClient) Close() error {
	client.streamClient.CloseSend()
	_, cancel := context.WithCancel(client.context)
	cancel()
	return client.conn.Close()
}
