package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

type RPCClient struct {
	conn       *grpc.ClientConn
	rpcClient  paustqproto.APIServiceClient
	zkClient   *zookeeper.ZKClient
	brokerPort uint16
	timeout    time.Duration
	Connected  bool
	logger     *logger.QLogger
}

func NewRPCClient(zkAddr string) *RPCClient {
	defaultTimeout := 3 * time.Second
	l := logger.NewQLogger("Rpc-client", logger.Info)

	return &RPCClient{
		zkClient:   zookeeper.NewZKClient(zkAddr),
		timeout:    defaultTimeout,
		Connected:  false,
		brokerPort: common.DefaultBrokerPort,
		logger:     l,
	}
}

func (client *RPCClient) WithTimeout(timeout time.Duration) *RPCClient {
	client.timeout = timeout
	return client
}

func (client *RPCClient) WithBrokerPort(port uint16) *RPCClient {
	client.brokerPort = port
	return client
}

func (client *RPCClient) WithLogLevel(level logger.LogLevel) *RPCClient {
	client.logger.SetLogLevel(level)
	return client
}

func (client *RPCClient) Connect() error {
	client.zkClient = client.zkClient.WithLogger(client.logger)
	err := client.zkClient.Connect()
	if err != nil {
		client.logger.Error(err)
		return err
	}

	var brokerAddr string

	brokers, err := client.zkClient.GetBrokers()
	if err != nil {
		client.logger.Error(err)
		client.zkClient.Close()
		return err
	}
	if brokers == nil {
		err := errors.New("broker doesn't exists")
		client.logger.Error(err)
		client.zkClient.Close()
		return err
	}

	// Get random broker
	randBrokerIndex := rand.Intn(len(brokers))
	brokerAddr = brokers[randBrokerIndex]
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddr, client.brokerPort)
	conn, err := grpc.Dial(brokerEndpoint, grpc.WithInsecure())
	if err != nil {
		client.logger.Error(err)
		client.zkClient.Close()
		return err
	}

	client.conn = conn
	client.rpcClient = paustqproto.NewAPIServiceClient(conn)
	client.Connected = true
	return nil
}

func (client *RPCClient) Close() {
	client.Connected = false
	client.conn.Close()
	client.zkClient.Close()
}

func (client *RPCClient) CreateTopic(ctx context.Context, topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {

	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	_, err := client.rpcClient.CreateTopic(c, message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor))
	if err != nil {
		client.logger.Error(err)
		return err
	}
	return nil
}

func (client *RPCClient) DeleteTopic(ctx context.Context, topicName string) error {

	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	_, err := client.rpcClient.DeleteTopic(c, message.NewDeleteTopicRequestMsg(topicName))
	if err != nil {
		client.logger.Error(err)
		return err
	}
	return nil
}

func (client *RPCClient) Heartbeat(ctx context.Context, msg string, brokerId uint64, brokerHost string) (*paustqproto.Pong, error) {

	c, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	return client.rpcClient.Heartbeat(c, message.NewPingMsg(msg, brokerId))
}
