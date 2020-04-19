package client

import (
	"context"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
)

type RpcClient struct {
	Conn        		*grpc.ClientConn
	ServerUrl			string
	Connected 			bool
}

func NewRpcClient(serverUrl string) *RpcClient {

	return &RpcClient{Connected: false, ServerUrl: serverUrl}
}

func (client *RpcClient) Connect() error {
	conn, err := grpc.Dial(client.ServerUrl, grpc.WithInsecure())
	if err != nil {
		return err
	}
	client.Conn = conn
	client.Connected = true

	return nil
}

func (client *RpcClient) Close() error {
	client.Connected = false
	return client.Conn.Close()
}

type TopicServiceRpcClient struct {
	context 			context.Context
	rpcClient			*RpcClient
	topicServiceClient 	paustqproto.TopicServiceClient
}

func NewTopicServiceRpcClient(context context.Context, serverUrl string) *TopicServiceRpcClient {
	return &TopicServiceRpcClient{rpcClient: NewRpcClient(serverUrl), context: context}
}

func (client *TopicServiceRpcClient) Connect() error {
	if err := client.rpcClient.Connect(); err != nil {
		return err
	}
	client.topicServiceClient = paustqproto.NewTopicServiceClient(client.rpcClient.Conn)
	return nil
}

func (client *TopicServiceRpcClient) Close() error {
	return client.rpcClient.Close()
}

func (client *TopicServiceRpcClient) CreateTopic(topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {
	_, err := client.topicServiceClient.CreateTopic(client.context, message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor))
	if err != nil {
		return err
	}
	return nil
}

func (client *TopicServiceRpcClient) DeleteTopic(topicName string) error {
	_, err := client.topicServiceClient.DeleteTopic(client.context, message.NewDeleteTopicRequestMsg(topicName))
	if err != nil {
		return err
	}
	return nil
}

func (client *TopicServiceRpcClient) DescribeTopic(topicName string) (*paustqproto.DescribeTopicResponse, error) {
	return client.topicServiceClient.DescribeTopic(client.context, message.NewDescribeTopicRequestMsg(topicName))
}

func (client *TopicServiceRpcClient) ListTopics() (*paustqproto.ListTopicsResponse, error) {
	return client.topicServiceClient.ListTopics(client.context, message.NewListTopicsRequestMsg())
}