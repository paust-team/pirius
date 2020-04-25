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
	Msg   *message.QMessage
}

type StreamClient struct {
	streamClient  	paustqproto.StreamService_FlowClient
	sockContainer 	*common.StreamSocketContainer
	SessionType   	paustqproto.SessionType
	conn          	*grpc.ClientConn
	ServerUrl     	string
	Connected     	bool
	MaxBufferSize 	uint32
}

func NewStreamClient(serverUrl string, sessionType paustqproto.SessionType) *StreamClient {
	return &StreamClient{SessionType: sessionType, ServerUrl: serverUrl, MaxBufferSize: 1024}
}

func (client *StreamClient) ReceiveToChan(receiveCh chan<- ReceivedData) {

	msg, err := client.Receive()
	receiveCh <- ReceivedData{err, msg}
}

func (client *StreamClient) Receive() (*message.QMessage, error) {
	return client.sockContainer.Read()
}

func (client *StreamClient) Send(msg *message.QMessage) error {
	return client.sockContainer.Write(msg, client.MaxBufferSize)
}

func (client *StreamClient) ConnectWithTopic(ctx context.Context, topicName string) error {

	if client.Connected {
		return errors.New("already connected")
	}

	conn, err := grpc.Dial(client.ServerUrl, grpc.WithInsecure())
	clientCtx, cancel := context.WithCancel(ctx)

	cancelAndClose := func() {
		conn.Close()
		cancel()
	}

	if err != nil {
		return err
	}
	client.conn = conn
	client.Connected = true

	stub := paustqproto.NewStreamServiceClient(conn)
	stream, err := stub.Flow(clientCtx)
	if err != nil {
		cancelAndClose()
		return err
	}

	sockContainer := common.NewSocketContainer(stream)
	reqMsg, err := message.NewQMessageFromMsg(message.NewConnectRequestMsg(client.SessionType, topicName))
	if err != nil {
		cancelAndClose()
		return err
	}

	if err := sockContainer.Write(reqMsg, client.MaxBufferSize); err != nil {
		cancelAndClose()
		return err
	}

	respMsg, err := sockContainer.Read()
	if err != nil {
		cancelAndClose()
		return err
	}

	connectResponseMsg := &paustqproto.ConnectResponse{}
	if err := respMsg.UnpackTo(connectResponseMsg); err != nil {
		cancelAndClose()
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
	return client.conn.Close()
}
