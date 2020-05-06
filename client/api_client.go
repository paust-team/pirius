package client

import (
	"context"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
	"time"
)

type APIClient struct {
	rpcClient paustqproto.APIServiceClient
	conn      *grpc.ClientConn
	ServerUrl string
	Connected bool
	timeout   time.Duration
}

func NewAPIClient(serverUrl string) *APIClient {
	defaultTimeout := 3 * time.Second
	return &APIClient{ServerUrl: serverUrl, Connected: false, timeout: defaultTimeout}
}

func (client *APIClient) WithTimeout(timeout time.Duration) *APIClient {
	client.timeout = timeout
	return client
}

func (client *APIClient) Connect() error {
	conn, err := grpc.Dial(client.ServerUrl, grpc.WithInsecure())
	if err != nil {
		return err
	}
	client.conn = conn
	client.Connected = true
	client.rpcClient = paustqproto.NewAPIServiceClient(conn)

	return nil
}

func (client *APIClient) Close() error {
	client.Connected = false
	return client.conn.Close()
}

func (client *APIClient) CreateTopic(ctx context.Context, topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {
	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	_, err := client.rpcClient.CreateTopic(c, message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor))
	if err != nil {
		return err
	}
	return nil
}

func (client *APIClient) DeleteTopic(ctx context.Context, topicName string) error {
	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	_, err := client.rpcClient.DeleteTopic(c, message.NewDeleteTopicRequestMsg(topicName))
	if err != nil {
		return err
	}
	return nil
}

func (client *APIClient) Heartbeat(ctx context.Context, msg string, brokerId uint64) (*paustqproto.Pong, error) {
	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	return client.rpcClient.Heartbeat(c, message.NewPingMsg(msg, brokerId))
}
