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
	streamClient  paustqproto.StreamService_FlowClient
	sockContainer *common.StreamSocketContainer
	SessionType   paustqproto.SessionType
	conn          *grpc.ClientConn
	ServerUrl     string
	Connected     bool
}

func NewStreamClient(serverUrl string, sessionType paustqproto.SessionType) *StreamClient {
	return &StreamClient{SessionType: sessionType, ServerUrl: serverUrl}
}

func (client *StreamClient) ContinuousRead() (<-chan common.Result, error) {
	if client.Connected {
		return client.sockContainer.ContinuousRead(), nil
	}
	return nil, errors.New("not connected")
}

func (client *StreamClient) Receive() (*message.QMessage, error) {
	if client.Connected {
		return client.sockContainer.Read()
	}
	return nil, errors.New("not connected")
}

func (client *StreamClient) Send(msg *message.QMessage) error {
	if client.Connected {
		return client.sockContainer.Write(msg)
	}
	return errors.New("not connected")
}

func (client *StreamClient) Connect(ctx context.Context, topicName string) error {

	if client.Connected {
		return errors.New("already connected")
	}

	conn, err := grpc.Dial(client.ServerUrl, grpc.WithInsecure())
	if err != nil {
		return err
	}

	client.conn = conn
	client.Connected = true

	stub := paustqproto.NewStreamServiceClient(conn)
	stream, err := stub.Flow(ctx)
	if err != nil {
		conn.Close()
		return err
	}

	sockContainer := common.NewSocketContainer(stream)
	reqMsg, err := message.NewQMessageFromMsg(message.NewConnectRequestMsg(client.SessionType, topicName))
	if err != nil {
		conn.Close()
		return err
	}

	if err := sockContainer.Write(reqMsg); err != nil {
		conn.Close()
		return err
	}

	respMsg, err := sockContainer.Read()
	if err != nil {
		conn.Close()
		return err
	}

	connectResponseMsg := &paustqproto.ConnectResponse{}
	if err := respMsg.UnpackTo(connectResponseMsg); err != nil {
		conn.Close()
		return err
	}

	client.conn = conn
	client.Connected = true
	client.streamClient = stream
	client.sockContainer = sockContainer

	return nil
}

func (client *StreamClient) Close() error {
	client.Connected = false
	client.streamClient.CloseSend()
	return client.conn.Close()
}
