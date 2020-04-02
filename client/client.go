package client

import (
	"context"
	"github.com/elon0823/paustq/paustqpb"
	"net"
	"time"
)

type TcpClient interface {
	Connect() error
	Close() error
}

type ReceivedData struct {
	Error error
	Data []byte
}

type Client struct {
	Ctx 		context.Context
	HostUrl 	string
	Timeout		time.Duration
	SessionType paustqpb.SessionType
	conn 		net.Conn

}

func NewClient(ctx context.Context, hostUrl string, timeout time.Duration) *Client {
	return &Client{Ctx: ctx, HostUrl: hostUrl, Timeout: timeout, conn: nil, }
}

func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.HostUrl, c.Timeout*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Close() error {
	_, cancel := context.WithCancel(c.Ctx)
	cancel()
	return c.conn.Close()
}

func (c *Client) Write(data []byte) error {
	_, err := c.conn.Write(data)
	return err
}

func (c *Client) Read(receiveCh chan <- ReceivedData) {

	readBuffer := make([]byte, 1024)
	n, err := c.conn.Read(readBuffer)
	if err != nil {
		receiveCh <- ReceivedData{err, nil}
	} else {
		receiveCh <- ReceivedData{err, readBuffer[0:n]}
	}
}
