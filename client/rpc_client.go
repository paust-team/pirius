package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

type RPCClient struct {
	zkClient   *zookeeper.ZKClient
	zkAddr     string
	brokerPort uint16
	timeout    time.Duration
	Connected  bool
}

func NewRPCClient(zkAddr string) *RPCClient {
	defaultTimeout := 3 * time.Second
	return &RPCClient{zkAddr: zkAddr, timeout: defaultTimeout, Connected: false, brokerPort: common.DefaultBrokerPort}
}

func (client *RPCClient) WithTimeout(timeout time.Duration) *RPCClient {
	client.timeout = timeout
	return client
}

func (client *RPCClient) WithBrokerPort(port uint16) *RPCClient {
	client.brokerPort = port
	return client
}

func (client *RPCClient) Connect() error {
	client.zkClient = zookeeper.NewZKClient(client.zkAddr)
	err := client.zkClient.Connect()
	if err != nil {
		return err
	}
	client.Connected = true
	return nil
}

func (client *RPCClient) Close() {
	client.Connected = false
	client.zkClient.Close()
}

func (client *RPCClient) CreateTopic(ctx context.Context, topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {

	var brokerAddr string

	brokers, err := client.zkClient.GetBrokers()
	if err != nil {
		return err
	}
	if brokers == nil {
		return errors.New("broker doesn't exists")
	}
	randBrokerIndex := rand.Intn(len(brokers))
	brokerAddr = brokers[randBrokerIndex]
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddr, client.brokerPort)
	conn, err := grpc.Dial(brokerEndpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	rpcClient := paustqproto.NewAPIServiceClient(conn)

	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	_, err = rpcClient.CreateTopic(c, message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor))
	if err != nil {
		return err
	}
	return nil
}

func (client *RPCClient) DeleteTopic(ctx context.Context, topicName string) error {

	brokers, err := client.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		return err
	}

	if brokers == nil {
		return errors.New("broker which has topic doesn't exists")
	}

	brokerEndpoint := fmt.Sprintf("%s:%d", brokers[0], client.brokerPort)
	conn, err := grpc.Dial(brokerEndpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	rpcClient := paustqproto.NewAPIServiceClient(conn)

	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	_, err = rpcClient.DeleteTopic(c, message.NewDeleteTopicRequestMsg(topicName))
	if err != nil {
		return err
	}
	return nil
}

func (client *RPCClient) Heartbeat(ctx context.Context, msg string, brokerId uint64, brokerHost string) (*paustqproto.Pong, error) {

	conn, err := grpc.Dial(brokerHost, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rpcClient := paustqproto.NewAPIServiceClient(conn)

	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	return rpcClient.Heartbeat(c, message.NewPingMsg(msg, brokerId))
}
