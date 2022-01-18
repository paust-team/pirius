package client

import (
	"context"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
	"math/rand"
	"net"
	"sync"
)

type ReceivedData struct {
	Error error
	Msg   *message.QMessage
}

type ClientBase struct {
	sync.Mutex
	socket           *network.Socket
	connected        bool
	config           *config.ClientConfigBase
	nodeId           string
	connectedAddress string
	zkClient         *zookeeper.ZKQClient
}

func newClientBase(config *config.ClientConfigBase, zkClient *zookeeper.ZKQClient) *ClientBase {
	return &ClientBase{
		Mutex:     sync.Mutex{},
		connected: false,
		config:    config,
		zkClient:  zkClient,
	}
}

func (c *ClientBase) isConnected() bool {
	c.Lock()
	defer c.Unlock()
	return c.connected
}

func (c *ClientBase) close() {
	c.Lock()
	if c.connected {
		c.socket.Close()
		c.socket = nil
		c.connected = false
	}
	c.zkClient.Close()
	c.Unlock()
}

func (c *ClientBase) continuousSend(ctx context.Context, writeCh <-chan *message.QMessage) (<-chan error, error) {
	if !c.isConnected() {
		return nil, pqerror.NotConnectedError{}
	}
	errCh := c.socket.ContinuousWrite(ctx, writeCh)
	return errCh, nil
}

func (c *ClientBase) send(msg *message.QMessage) error {
	if !c.isConnected() {
		return pqerror.NotConnectedError{}
	}
	return c.socket.Write(msg)
}

func (c *ClientBase) continuousReceive(ctx context.Context) (<-chan *message.QMessage, <-chan error, error) {
	if !c.isConnected() {
		return nil, nil, pqerror.NotConnectedError{}
	}

	msgCh, errCh := c.socket.ContinuousRead(ctx)
	return msgCh, errCh, nil
}

func (c *ClientBase) receive() (*message.QMessage, error) {
	if !c.isConnected() {
		return nil, pqerror.NotConnectedError{}
	}
	return c.socket.Read()
}

func (c *ClientBase) connectToBroker(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return pqerror.DialFailedError{Addr: address, Err: err}
	}
	c.socket = network.NewSocket(conn, c.config.BrokerTimeout(), c.config.BrokerTimeout())

	c.Lock()
	c.connected = true
	c.Unlock()

	return nil
}

func (c *ClientBase) connect(sessionType shapleqproto.SessionType, topic string) error {
	if len(topic) == 0 {
		return pqerror.TopicNotSetError{}
	}
	if err := c.zkClient.Connect(); err != nil {
		return err
	}

	if c.isConnected() {
		return pqerror.AlreadyConnectedError{Addr: c.connectedAddress}
	}

	var fragmentId uint32 = 0 // TODO:: get fragment ids and connect to fragments
	topicBrokerAddrs, err := c.zkClient.GetTopicFragmentBrokers(topic, fragmentId)
	if err != nil {
		return pqerror.ZKOperateError{ErrStr: err.Error()}
	}
	if len(topicBrokerAddrs) > 0 {
		if err := c.connectToBroker(topicBrokerAddrs[0]); err != nil {
			return err
		}
	} else if sessionType == shapleqproto.SessionType_PUBLISHER { // if publisher, pick random broker unless topic broker exists
		brokerAddrs, err := c.zkClient.GetBrokers()
		if err == nil && len(brokerAddrs) != 0 {
			brokerAddr := brokerAddrs[rand.Intn(len(brokerAddrs))] // pick random broker
			if err := c.connectToBroker(brokerAddr); err != nil {
				return err
			}
		} else {
			return pqerror.TopicFragmentNotExistsError{}
		}
	} else {
		return pqerror.TopicFragmentNotExistsError{}
	}

	if err := c.initStream(sessionType, topic); err != nil {
		return err
	}
	return nil
}

func (c *ClientBase) initStream(sessionType shapleqproto.SessionType, topic string) error {
	reqMsg, err := message.NewQMessageFromMsg(message.STREAM, message.NewConnectRequestMsg(sessionType, topic))
	if err != nil {
		return err
	}
	if err := c.send(reqMsg); err != nil {
		return err
	}
	res, err := c.receive()
	if err != nil {
		return err
	}
	if _, err := res.UnpackTo(&shapleqproto.ConnectResponse{}); err != nil {
		return err
	}
	return nil
}
