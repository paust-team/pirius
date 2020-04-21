package client

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
)

type ReceivedData struct {
	Error error
	Msg  *message.QMessage
}

type StreamClient struct {
	context 			context.Context
	rpcClient 			*RpcClient
	stub 				paustqproto.StreamServiceClient
	sockContainer	 	*common.StreamSocketContainer
	clientStream 		paustqproto.StreamService_FlowClient
	SessionType 		paustqproto.SessionType
}

func NewStreamClient(context context.Context, serverUrl string, sessionType paustqproto.SessionType) *StreamClient {
	return &StreamClient{rpcClient: NewRpcClient(serverUrl), context: context, SessionType: sessionType}
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
	if client.rpcClient.Connected {
		return errors.New("already connected")
	}

	if err := client.rpcClient.Connect(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(client.context)
	stub := paustqproto.NewStreamServiceClient(client.rpcClient.Conn)
	stream, err := stub.Flow(ctx)
	if err != nil {
		client.rpcClient.Close()
		cancel()
		return err
	}

	sockContainer := common.NewSocketContainer(stream)
	reqMsg, err := message.NewQMessageFromMsg(message.NewConnectRequestMsg(client.SessionType, topicName))
	if err != nil {
		client.rpcClient.Close()
		cancel()
		return err
	}

	if err:= sockContainer.Write(reqMsg); err != nil {
		client.rpcClient.Close()
		cancel()
		return err
	}

	respMsg, err := sockContainer.Read()
	if err != nil {
		client.rpcClient.Close()
		cancel()
		return err
	}

	connectResponseMsg := &paustqproto.ConnectResponse{}
	if err := respMsg.UnpackTo(connectResponseMsg); err != nil {
		client.rpcClient.Close()
		cancel()
		return err
	}

	client.clientStream = stream
	client.sockContainer = sockContainer
	client.stub = stub

	return nil
}

func (client *StreamClient) Close() error {
	client.clientStream.CloseSend()
	_, cancel := context.WithCancel(client.context)
	cancel()
	return client.rpcClient.Close()
}
