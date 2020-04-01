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

type Client struct {
	Ctx 		context.Context
	HostUrl 	string
	Timeout		time.Duration
	SessionType paustqpb.SessionType
	conn 		net.Conn
	buffer 		[]byte
}

type ResultData struct {
	Error error
	Data []byte
}

func NewClient(ctx context.Context, hostUrl string, timeout time.Duration) *Client {
	return &Client{Ctx: ctx, HostUrl: hostUrl, Timeout: timeout, conn: nil, buffer: make([]byte, 1024)}
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

func (c *Client) Read(outCh chan <- ResultData) {

	n, err := c.conn.Read(c.buffer)
	if err != nil {
		outCh <- ResultData{err, nil}
	} else {
		outCh <- ResultData{err, c.buffer[0:n]}
	}
}
