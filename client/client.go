package client

import (
	"context"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"log"
	"net"
	"time"
)

type TcpClient interface {
	Connect() error
	Close() error
}

type ReceivedData struct {
	Error error
	Data  []byte
}

type Client struct {
	ctx         context.Context
	conn        net.Conn
	Timeout     time.Duration
	HostUrl     string
	SessionType paustq_proto.SessionType
	Connected   bool
}

func NewClient(ctx context.Context, hostUrl string, timeout time.Duration, sessionType paustq_proto.SessionType) *Client {
	return &Client{ctx: ctx, HostUrl: hostUrl, Timeout: timeout, SessionType: sessionType, conn: nil, Connected: false}
}

func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.HostUrl, c.Timeout*time.Second)
	if err != nil {
		return err
	}

	c.conn = conn

	protoMsg, protoErr := message.NewConnectMsg(c.SessionType)
	if protoErr != nil {
		log.Fatal("Failed to create Connect message")
		return c.Close()

	}
	connReqErr := c.Write(protoMsg)
	if connReqErr != nil {
		log.Fatal("Failed to send connect request to broker")
		return c.Close()
	}

	c.Connected = true

	return nil
}

func (c *Client) Close() error {
	_, cancel := context.WithCancel(c.ctx)
	cancel()
	return c.conn.Close()
}

func (c *Client) Write(data []byte) error {
	_, err := c.conn.Write(data)
	return err
}

func (c *Client) Read(receiveCh chan<- ReceivedData) {

	readBuffer := make([]byte, 1024)
	n, err := c.conn.Read(readBuffer)
	if err != nil {
		receiveCh <- ReceivedData{err, nil}
	} else {
		receiveCh <- ReceivedData{err, readBuffer[0:n]}
	}
}
