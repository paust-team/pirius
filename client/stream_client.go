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
	streamClient 		paustqproto.StreamService_FlowClient
	sockContainer	 	*common.StreamSocketContainer
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
	return client.sockContainer.Read()
}

func (client *StreamClient) Send(msg *message.QMessage) error {
	return client.sockContainer.Write(msg)
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
	stream, err := stub.Flow(ctx)
	if err != nil {
		conn.Close()
		cancel()
		return err
	}

	sockContainer := common.NewSocketContainer(stream)
	reqMsg, err := message.NewQMessageFromMsg(message.NewConnectRequestMsg(client.SessionType, topicName))
	if err != nil {
		conn.Close()
		cancel()
		return err
	}

	if err:= sockContainer.Write(reqMsg); err != nil {
		conn.Close()
		cancel()
		return err
	}

	respMsg, err := sockContainer.Read()
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
	client.sockContainer = sockContainer

	return nil
}

func (client *StreamClient) Close() error {
	client.streamClient.CloseSend()
	_, cancel := context.WithCancel(client.context)
	cancel()
	return client.conn.Close()
}
