package client

import (
	"context"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/utils"
	"github.com/paust-team/shapleq/zookeeper"
	"net"
	"sync"
)

type connectionTarget struct {
	address     string
	topic       string
	fragmentIds []uint32
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
	zkClient         *zookeeper.ZKQClient
}

func newClient(config *config.ClientConfigBase, zkClient *zookeeper.ZKQClient) *client {
	return &client{
		Mutex:       sync.Mutex{},
		connected:   false,
		config:      config,
		zkClient:    zkClient,
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

	c.zkClient.Close()
	c.Unlock()
}

func (c *client) continuousSend(ctx context.Context, writeCh <-chan *messageAndConnection) (<-chan error, error) {
	if !c.isConnected() {
		return nil, pqerror.NotConnectedError{}
	}
	errCh := make(chan error)

	for _, conn := range c.connections {
		go func(cn *connection) {
			cn.writeCh = make(chan *message.QMessage)
			for err := range cn.socket.ContinuousWrite(ctx, cn.writeCh) {
				errCh <- err
			}
		}(conn)
	}

	go func() {
		defer close(errCh)
		defer func() {
			for _, connection := range c.connections {
				close(connection.writeCh)
			}
		}()
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

	for _, conn := range c.connections {
		go func(cn *connection) {
			cn.readCh = make(chan *message.QMessage)
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

	return msgCh, errCh, nil
}

func (c *client) connect(sessionType shapleqproto.SessionType, targets []*connectionTarget) error {
	if c.isConnected() {
		return pqerror.AlreadyConnectedError{Addr: c.connectedAddress}
	}
	if err := c.zkClient.Connect(); err != nil {
		return err
	}

	for _, target := range targets {
		if len(target.topic) == 0 {
			return pqerror.TopicNotSetError{}
		}
		if err := c.establishStream(sessionType, target); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) establishStream(sessionType shapleqproto.SessionType, target *connectionTarget) error {
	conn, err := net.Dial("tcp", target.address)
	if err != nil {
		return pqerror.DialFailedError{Addr: target.address, Err: err}
	}
	sock := network.NewSocket(conn, c.config.BrokerTimeout(), c.config.BrokerTimeout())
	reqMsg, err := message.NewQMessageFromMsg(message.STREAM,
		message.NewConnectRequestMsg(sessionType, target.topic, target.fragmentIds))
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
		if conn.topic == topic && utils.Contains(conn.fragmentIds, fragmentId) {
			connections = append(connections, conn)
		}
	}
	return connections
}
