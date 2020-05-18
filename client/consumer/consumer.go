package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"time"
)

type Consumer struct {
	done        chan bool
	client      *client.StreamClient
	subscribing bool
	timeout     time.Duration
	zkClient    *zookeeper.ZKClient
	brokerPort  uint16
	logger      *logger.QLogger
}

type SinkData struct {
	Error              error
	Data               []byte
	Offset, LastOffset uint64
}

func NewConsumer(zkHost string) *Consumer {
	l := logger.NewQLogger("Consumer", logger.Info)
	return &Consumer{
		zkClient:    zookeeper.NewZKClient(zkHost),
		subscribing: false,
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

func (c *Consumer) waitSubscribed(ctx context.Context) chan client.ReceivedData {

	c.logger.Debug("start waiting subscribe response msg.")

	onReceiveResponse := make(chan client.ReceivedData)

	go func() {
		defer close(onReceiveResponse)
		for {
			msg, err := c.client.Receive()
			select {
			case onReceiveResponse <- client.ReceivedData{Error: err, Msg: msg}:
				c.logger.Debug("received subscribe response.")
			case <-c.done:
				c.logger.Debug("done channel closed. stop waitSubscribe")
				return
			case <-ctx.Done():
				c.logger.Debug("received ctx done. stop waitSubscribe")
				return
			}
		}
	}()
	return onReceiveResponse
}

func (c *Consumer) startSubscribe(ctx context.Context) chan SinkData {

	if !c.subscribing {
		return nil
	}

	c.logger.Info("start subscribe.")

	c.done = make(chan bool)
	sinkChannel := make(chan SinkData)

	onReceiveChan := c.waitSubscribed(ctx)

	go func() {

		defer c.logger.Info("end subscribe")
		defer close(c.done)
		defer close(sinkChannel)

		for {
			select {
			case res := <-onReceiveChan:
				if res.Error != nil {
					c.logger.Error(res.Error)
					sinkChannel <- SinkData{Error: res.Error}
					return
				} else if res.Msg == nil { // stream finished
					c.logger.Debug("subscribe stream finished")
					return
				} else {
					fetchRespMsg := &paustqproto.FetchResponse{}
					if err := res.Msg.UnpackTo(fetchRespMsg); err != nil {
						sinkChannel <- SinkData{Error: err}
						return
					}
					sinkChannel <- SinkData{nil, fetchRespMsg.Data, fetchRespMsg.Offset, fetchRespMsg.LastOffset}
					c.logger.Debug("subscribe data ->", fetchRespMsg.Data, "offset ->", fetchRespMsg.Offset, "last offset ->", fetchRespMsg.LastOffset)
				}
			case <-c.done:
				c.logger.Debug("done channel closed. stop subscribe")
				return
			case <-ctx.Done():
				c.logger.Debug("received ctx done. stop subscribe")
				return
			}
		}
	}()

	return sinkChannel
}

func (c *Consumer) Subscribe(ctx context.Context, startOffset uint64) (chan SinkData, error) {

	if c.subscribing == false {
		c.subscribing = true

		sinkChan := c.startSubscribe(ctx)

		reqMsg, err := message.NewQMessageFromMsg(message.NewFetchRequestMsg(startOffset))
		if err != nil {
			c.logger.Error(err)
			return nil, c.Close()
		}
		if err = c.client.Send(reqMsg); err != nil {
			c.logger.Error(err)
			return nil, c.Close()
		}
		return sinkChan, nil
	} else {
		err := errors.New("already subscribing")
		c.logger.Error(err)
		return nil, err
	}
}

func (c *Consumer) Connect(ctx context.Context, topicName string) error {
	c.zkClient = c.zkClient.WithLogger(c.logger)
	if err := c.zkClient.Connect(); err != nil {
		c.logger.Error(err)
		return err
	}

	brokerAddrs, err := c.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		c.logger.Error(err)
		return err
	}
	if brokerAddrs == nil {
		err := errors.New("topic doesn't exists")
		c.logger.Error(err)
		return err
	}
	// TODO:: Support partition for topic
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddrs[0], c.brokerPort)
	c.client = client.NewStreamClient(brokerEndpoint, paustqproto.SessionType_SUBSCRIBER)

	if err = c.client.Connect(ctx, topicName); err != nil {
		c.logger.Error(err)
		return err
	}

	c.logger.Info("consumer is connected")
	return nil
}

func (c *Consumer) Close() error {
	c.subscribing = false
	c.done <- true
	c.zkClient.Close()

	if err := c.client.Close(); err != nil {
		c.logger.Error(err)
		return err
	}

	c.logger.Info("consumer is closed")
	return nil
}
