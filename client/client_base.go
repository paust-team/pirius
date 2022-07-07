package client

import (
	"context"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/utils"
	"net"
	"sync"
)

type connectionTarget struct {
	address string
	topics  []*common.Topic
}

func (c connectionTarget) findTopicFragments(topic string) *common.Topic {
	for _, topicFragment := range c.topics {
		if topicFragment.TopicName() == topic {
			return topicFragment
		}
	}

	return nil
}

type connection struct {
	*connectionTarget
	connected bool
	readCh    chan *message.QMessage
	writeCh   chan *message.QMessage
	socket    *network.Socket
}

func (c *connection) Close() {
	c.socket.Close()
	c.connected = true
}

type messageAndConnection struct {
	message    *message.QMessage
	connection *connection
}

type ReceivedData struct {
	Error error
	Msg   *message.QMessage
}

type client struct {
	sync.Mutex
	connections      map[string]*connection
	connected        bool
	config           *config.ClientConfigBase
	nodeId           string
	connectedAddress string
}

func newClient(config *config.ClientConfigBase) *client {
	return &client{
		Mutex:       sync.Mutex{},
		connected:   false,
		config:      config,
		connections: make(map[string]*connection),
	}
}

func (c *client) isConnected() bool {
	c.Lock()
	defer c.Unlock()
	return c.connected
}

func (c *client) close() {
	c.Lock()
	if c.connected {
		for _, conn := range c.connections {
			conn.Close()
		}
		c.connected = false
	}
	c.Unlock()
}

func (c *client) continuousSend(ctx context.Context, writeCh <-chan *messageAndConnection) (<-chan error, error) {
	if !c.isConnected() {
		return nil, pqerror.NotConnectedError{}
	}
	errCh := make(chan error)
	wg := sync.WaitGroup{}

	for _, conn := range c.connections {
		wg.Add(1)
		go func(cn *connection) {
			defer wg.Done()
			cn.writeCh = make(chan *message.QMessage)
			defer close(cn.writeCh)

			for err := range cn.socket.ContinuousWrite(ctx, cn.writeCh) {
				errCh <- err
			}
		}(conn)
	}

	go func() {
		defer close(errCh)
		defer wg.Wait()

		for {
			select {
			case <-ctx.Done():
				return
			case msgAndConn, ok := <-writeCh:
				if !ok {
					return
				}
				msgAndConn.connection.writeCh <- msgAndConn.message
			}
		}
	}()

	return errCh, nil
}

func (c *client) continuousReceive(ctx context.Context) (<-chan *messageAndConnection, <-chan error, error) {
	if !c.isConnected() {
		return nil, nil, pqerror.NotConnectedError{}
	}
	msgCh := make(chan *messageAndConnection)
	errCh := make(chan error)
	wg := sync.WaitGroup{}

	for _, conn := range c.connections {
		wg.Add(1)
		go func(cn *connection) {
			defer wg.Done()
			cn.readCh = make(chan *message.QMessage)
			defer close(cn.readCh)

			socketMsgCh, socketErrCh := cn.socket.ContinuousRead(ctx)
			for {
				select {
				case msg, ok := <-socketMsgCh:
					if !ok {
						return
					}
					msgCh <- &messageAndConnection{msg, cn}
				case err, ok := <-socketErrCh:
					if !ok {
						return
					}
					errCh <- err
				}
			}
		}(conn)
	}

	go func() {
		defer close(errCh)
		defer close(msgCh)
		defer wg.Wait()

		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}()

	return msgCh, errCh, nil
}

func (c *client) connect(sessionType shapleqproto.SessionType, targets []*connectionTarget) error {
	if c.isConnected() {
		return pqerror.AlreadyConnectedError{Addr: c.connectedAddress}
	}

	for _, target := range targets {
		if err := c.establishStream(sessionType, target); err != nil {
			return err
		}
	}
	c.connected = true
	return nil
}

func (c *client) establishStream(sessionType shapleqproto.SessionType, target *connectionTarget) error {
	conn, err := net.Dial("tcp", target.address)
	if err != nil {
		return pqerror.DialFailedError{Addr: target.address, Err: err}
	}
	sock := network.NewSocket(conn, c.config.BrokerTimeout(), c.config.BrokerTimeout())
	reqMsg, err := message.NewQMessageFromMsg(message.STREAM,
		message.NewConnectRequestMsg(sessionType, target.topics))
	if err != nil {
		return err
	}
	if err := sock.Write(reqMsg); err != nil {
		return err
	}
	res, err := sock.Read()
	if err != nil {
		return err
	}
	if _, err := res.UnpackTo(&shapleqproto.ConnectResponse{}); err != nil {
		return err
	}
	c.connections[target.address] = &connection{connectionTarget: target, socket: sock}

	return nil
}

func (c *client) getConnections(topic string, fragmentId uint32) []*connection {
	var connections []*connection
	for _, conn := range c.connections {
		for _, topicFragments := range conn.topics {
			if topicFragments.TopicName() == topic && utils.Contains(topicFragments.FragmentIds(), fragmentId) {
				connections = append(connections, conn)
			}
		}
	}
	return connections
}
