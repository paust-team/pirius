package client

import (
	"context"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
)

type APIClient struct {
	context 			context.Context
	rpcClient			paustqproto.APIServiceClient
	conn 				*grpc.ClientConn
	ServerUrl			string
	Connected 			bool
}

func NewAPIClient(context context.Context, serverUrl string) *APIClient {
	return &APIClient{ServerUrl: serverUrl, context: context, Connected: false}
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

func (client *APIClient) CreateTopic(topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {
	_, err := client.rpcClient.CreateTopic(client.context, message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor))
	if err != nil {
		return err
	}
	return nil
}

func (client *APIClient) DeleteTopic(topicName string) error {
	_, err := client.rpcClient.DeleteTopic(client.context, message.NewDeleteTopicRequestMsg(topicName))
	if err != nil {
		return err
	}
	return nil
}

func (client *APIClient) DescribeTopic(topicName string) (*paustqproto.DescribeTopicResponse, error) {
	return client.rpcClient.DescribeTopic(client.context, message.NewDescribeTopicRequestMsg(topicName))
}

func (client *APIClient) ListTopics() (*paustqproto.ListTopicsResponse, error) {
	return client.rpcClient.ListTopics(client.context, message.NewListTopicsRequestMsg())
}

func (client *APIClient) Heartbeat(msg string, brokerId uint64) (*paustqproto.Pong, error) {
	return client.rpcClient.Heartbeat(client.context, message.NewPingMsg(msg, brokerId))
}