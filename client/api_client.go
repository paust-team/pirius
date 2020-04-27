package client

import (
	"context"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
)

type APIClient struct {
	rpcClient paustqproto.APIServiceClient
	conn      *grpc.ClientConn
	ServerUrl string
	Connected bool
}

func NewAPIClient(serverUrl string) *APIClient {
	return &APIClient{ServerUrl: serverUrl, Connected: false}
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
	_, err := client.rpcClient.CreateTopic(ctx, message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor))
	if err != nil {
		return err
	}
	return nil
}

func (client *APIClient) DeleteTopic(ctx context.Context, topicName string) error {
	_, err := client.rpcClient.DeleteTopic(ctx, message.NewDeleteTopicRequestMsg(topicName))
	if err != nil {
		return err
	}
	return nil
}

func (client *APIClient) DescribeTopic(ctx context.Context, topicName string) (*paustqproto.DescribeTopicResponse, error) {
	return client.rpcClient.DescribeTopic(ctx, message.NewDescribeTopicRequestMsg(topicName))
}

func (client *APIClient) ListTopics(ctx context.Context) (*paustqproto.ListTopicsResponse, error) {
	return client.rpcClient.ListTopics(ctx, message.NewListTopicsRequestMsg())
}

func (client *APIClient) Heartbeat(ctx context.Context, msg string, brokerId uint64) (*paustqproto.Pong, error) {
	return client.rpcClient.Heartbeat(ctx, message.NewPingMsg(msg, brokerId))
}
