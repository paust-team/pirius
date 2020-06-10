package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"sync"
	"time"
)

type Consumer struct {
	mu            *sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	client      *client.StreamClient
	timeout     time.Duration
	zkClient    *zookeeper.ZKClient
	brokerPort  uint16
	logger      *logger.QLogger
}

type FetchData struct {
	Data               []byte
	Offset, LastOffset uint64
}

func NewConsumer(zkHost string) *Consumer {
	l := logger.NewQLogger("Consumer", logger.Info)
	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		mu: &sync.Mutex{},
		ctx: 		ctx,
		cancel:		cancel,
		zkClient:    zookeeper.NewZKClient(zkHost),
		brokerPort:  common.DefaultBrokerPort,
		logger:      l,
	}
}

func (c *Consumer) WithLogLevel(level logger.LogLevel) *Consumer {
	c.logger.SetLogLevel(level)
	return c
}

func (c *Consumer) WithBrokerPort(port uint16) *Consumer {
	c.brokerPort = port
	return c
}

func (c *Consumer) WithTimeout(timeout time.Duration) *Consumer {
	c.timeout = timeout
	return c
}

func (c *Consumer) Subscribe(ctx context.Context, startOffset uint64) (<- chan FetchData, <- chan error) {

	c.logger.Info("start subscribe.")

	sinkCh := make(chan FetchData)
	errCh := make(chan error)

	go func() {

		defer c.logger.Info("end subscribe")
		defer close(sinkCh)
		defer close(errCh)

		doneRecvCh := make(chan bool)
		defer close(doneRecvCh)

		recvCh, recvErrCh := c.client.ContinuousRead()

		msgHandler := client.MessageHandler{}
		msgHandler.RegisterMsgHandle(&paustqproto.FetchResponse{}, func(msg proto.Message) {
			res := msg.(*paustqproto.FetchResponse)
			sinkCh <- FetchData{res.Data, res.Offset, res.LastOffset}
			c.logger.Debug("subscribe data ->", res.Data, "offset ->", res.Offset, "last offset ->", res.LastOffset)

		})
		msgHandler.RegisterMsgHandle(&paustqproto.Ack{}, func(msg proto.Message) {
			ack := msg.(*paustqproto.Ack)
			err := errors.New(fmt.Sprintf("received subscribe ack with error code %d", ack.Code))
			errCh <- err
		})

		reqMsg, err := message.NewQMessageFromMsg(message.NewFetchRequestMsg(startOffset))
		if err != nil {
			c.logger.Error(err)
			errCh <- err
			return
		}
		if err = c.client.Send(reqMsg); err != nil {
			c.logger.Error(err)
			errCh <- err
			return
		}

		for {
			select {
			case msg := <-recvCh:
				if msg != nil {
					if err := msgHandler.Handle(msg); err != nil {
						errCh <- err
					}
				}
			case err := <-recvErrCh:
				errCh <- err
			case <-c.ctx.Done():
				c.logger.Debug("consumer closed. stop subscribe")
				return

			case <-ctx.Done():
				c.logger.Debug("received ctx done. stop subscribe")
				c.Close()
				return
			}
		}
	}()

	return sinkCh, errCh
}

func (c *Consumer) Connect(ctx context.Context, topicName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil && c.client.Connected {
		return errors.New("already connected")
	}

	c.zkClient = c.zkClient.WithLogger(c.logger)
	if err := c.zkClient.Connect(); err != nil {
		c.logger.Error(err)
		return err
	}

	brokerAddrs, err := c.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		c.logger.Error(err)
		c.zkClient.Close()
		return err
	}
	if brokerAddrs == nil {
		err := errors.New("topic doesn't exists")
		c.logger.Error(err)
		c.zkClient.Close()
		return err
	}
	// TODO:: Support partition for topic
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddrs[0], c.brokerPort)
	c.client = client.NewStreamClient(brokerEndpoint, paustqproto.SessionType_SUBSCRIBER)

	if err = c.client.Connect(ctx, topicName); err != nil {
		c.logger.Error(err)
		c.zkClient.Close()
		return err
	}

	c.logger.Info("consumer is connected")
	return nil
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil && c.client.Connected {

		c.cancel()
		c.zkClient.Close()

		if err := c.client.Close(); err != nil {
			c.logger.Error(err)
			return err
		}

		c.logger.Info("consumer is closed")
	}

	return nil
}
