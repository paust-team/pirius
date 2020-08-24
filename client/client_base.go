package client

import (
	"errors"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"net"
	"sync"
)

type ReceivedData struct {
	Error error
	Msg   *message.QMessage
}

type ClientBase struct {
	sync.Mutex
	socket    *network.Socket
	connected bool
	config    *config.ClientConfigBase
}

func newClientBase(config *config.ClientConfigBase) *ClientBase {
	return &ClientBase{
		Mutex:     sync.Mutex{},
		connected: false,
		config:    config,
	}
}

func (c *ClientBase) isConnected() bool {
	c.Lock()
	defer c.Unlock()
	return c.connected
}

func (c *ClientBase) connectToBroker(brokerAddr string) error {
	if c.isConnected() {
		return pqerror.AlreadyConnectedError{Addr: brokerAddr}
	}

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return pqerror.DialFailedError{Addr: brokerAddr, Err: err}
	}
	c.socket = network.NewSocket(conn, c.config.Timeout(), c.config.Timeout())

	c.Lock()
	c.connected = true
	c.Unlock()

	return nil
}

func (c *ClientBase) close() {
	c.Lock()
	if c.connected {
		c.socket.Close()
		c.socket = nil
		c.connected = false
	}
	c.Unlock()
}

func (c *ClientBase) continuousReadWrite() (<-chan *message.QMessage, chan<- *message.QMessage, <-chan error, error) {
	if !c.isConnected() {
		return nil, nil, nil, pqerror.NotConnectedError{}
	}
	readCh, writeCh, errCh := c.socket.ContinuousReadWrite()
	return readCh, writeCh, errCh, nil
}

func (c *ClientBase) send(msg *message.QMessage) error {
	if !c.isConnected() {
		return pqerror.NotConnectedError{}
	}
	return c.socket.Write(msg)
}

func (c *ClientBase) receive() (*message.QMessage, error) {
	if !c.isConnected() {
		return nil, pqerror.NotConnectedError{}
	}
	return c.socket.Read()
}

func (c *ClientBase) connect(sessionType shapleqproto.SessionType, topic string) error {
	if len(topic) == 0 {
		return pqerror.TopicNotSetError{}
	}

	err := c.connectToBroker(c.config.BrokerAddr())
	if err != nil {
		return err
	}

	req, err := message.NewQMessageFromMsg(message.TRANSACTION, message.NewDiscoverBrokerRequestMsg(topic, sessionType))
	if err != nil {
		return err
	}

	if err := c.send(req); err != nil {
		return err
	}

	res, err := c.receive()
	if err != nil {
		return err
	}

	discoverRes := &shapleqproto.DiscoverBrokerResponse{}
	err = res.UnpackTo(discoverRes)
	if err != nil {
		return err
	}

	if pqerror.PQCode(discoverRes.ErrorCode) != pqerror.Success {
		return errors.New(discoverRes.ErrorMessage)
	}

	newAddr := discoverRes.GetAddress()
	c.close()
	if err = c.connectToBroker(newAddr); err != nil {
		return err
	}

	if err = c.initStream(sessionType, topic); err != nil {
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
	connectRes := &shapleqproto.ConnectResponse{}
	if err := res.UnpackTo(connectRes); err != nil {
		return err
	}
	return nil
}
