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
	SessionType paustq_proto.SessionType
	conn 		net.Conn
	buffer 		[]byte
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

func (c *Client) read(outCh chan <- []byte, errCh chan <- error) {

	n, err := c.conn.Read(c.buffer)
	if err != nil {
		errCh <- err
	} else {
		outCh <- c.buffer[0:n]
	}
}

func (c *Client) Read(outCh chan <- []byte, errCh chan <- error) {

	readCh := make(chan []byte)
	readErrCh := make(chan error)

	go c.read(readCh, readErrCh)

	for {
		select {
		case <- c.Ctx.Done():
			return
		case data := <-readCh:
			outCh <- data
		case err := <-readErrCh:
			errCh <- err
		}
	}
}