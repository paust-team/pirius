package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/proto"
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

func (c *Client) Connect(topicName string) error {
	conn, err := net.DialTimeout("tcp", c.HostUrl, c.Timeout*time.Second)
	if err != nil {
		return err
	}

	c.conn = conn
	c.Connected = true

	requestData, err := message.NewConnectRequestMsgData(c.SessionType, topicName)
	if err != nil {
		log.Fatal("Failed to create Connect message")
		_ = conn.Close()
		return err
	}

	if c.Write(requestData) != nil {
		log.Fatal("Failed to send connect request to broker")
		_ = conn.Close()
		return err
	}

	data, err := c.Read(3)

	if err != nil {
		_ = conn.Close()
		return err
	}

	connectRespMsg := &paustq_proto.ConnectResponse{}
	if message.UnPackTo(data, connectRespMsg) != nil {
		_ = conn.Close()
		return errors.New("failed to parse data to PutResponse")
	} else if connectRespMsg.ErrorCode != 0 {
		_ = conn.Close()
		return errors.New(fmt.Sprintf("connectRequest has error code with %d", connectRespMsg.ErrorCode))
	}

	return nil
}

func (c *Client) Close() error {
	c.Connected = false
	return c.conn.Close()
}

func (c *Client) Write(msgData []byte) error {
	if c.Connected {
		data, err := message.Serialize(msgData)
		if err != nil {
			return err
		}
		_, err = c.conn.Write(data)
		return err
	}
	return errors.New("disconnected")
}

func (c *Client) Read(timeout time.Duration) ([]byte, error) {

	if c.Connected {
		c.conn.SetReadDeadline(time.Now().Add(timeout * time.Second))

		var totalData []byte
		for {
			readBuffer := make([]byte, 1024)
			n, err := c.conn.Read(readBuffer)
			if err != nil {
				return nil, err
			}

			totalData = append(totalData, readBuffer[:n]...)
			data, err := message.Deserialize(totalData)

			if err != nil {
				if data != nil {
					continue
				}
				return nil, err
			}

			return data, nil
		}
	}
	return nil, errors.New("disconnected")
}

func (c *Client) ReadToChan(receiveCh chan<- ReceivedData, timeout time.Duration) {

	c.conn.SetReadDeadline(time.Now().Add(timeout * time.Second))

	data, err := c.Read(timeout)
	if err != nil {
		receiveCh <- ReceivedData{err, nil}
	} else {
		if err != nil {
			receiveCh <- ReceivedData{err, nil}
		} else {
			receiveCh <- ReceivedData{nil, data}
		}
	}
}
