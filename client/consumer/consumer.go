package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"log"
	"time"
)

type Consumer struct {
	done        chan bool
	client      *client.StreamClient
	subscribing bool
	timeout     time.Duration
	zkClient    *zookeeper.ZKClient
	brokerPort  uint16
}

type SinkData struct {
	Error              error
	Data               []byte
	Offset, LastOffset uint64
}

func NewConsumer(zkHost string) *Consumer {
	return &Consumer{zkClient: zookeeper.NewZKClient(zkHost), subscribing: false, brokerPort: common.DefaultBrokerPort}
}

func (c *Consumer) WithBrokerPort(port uint16) *Consumer {
	c.brokerPort = port
	return c
}

func (c *Consumer) WithTimeout(timeout time.Duration) *Consumer {
	c.timeout = timeout
	return c
}

func (c *Consumer) waitResponse(ctx context.Context) chan client.ReceivedData {

	onReceiveResponse := make(chan client.ReceivedData)

	go func() {
		defer close(onReceiveResponse)
		for {
			msg, err := c.client.Receive()
			select {
			case onReceiveResponse <- client.ReceivedData{Error: err, Msg: msg}:
			case <-c.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return onReceiveResponse
}

func (c *Consumer) startSubscribe(ctx context.Context) chan SinkData {
	c.done = make(chan bool)
	sinkChannel := make(chan SinkData)

	onReceiveChan := c.waitResponse(ctx)

	go func() {
		defer close(c.done)
		defer close(sinkChannel)

		if !c.subscribing {
			return
		}

		for {
			select {
			case res := <-onReceiveChan:
				if res.Error != nil {
					sinkChannel <- SinkData{Error: res.Error}
					return
				} else if res.Msg == nil { // stream finished
					return
				} else {
					fetchRespMsg := &paustqproto.FetchResponse{}
					if err := res.Msg.UnpackTo(fetchRespMsg); err != nil {
						sinkChannel <- SinkData{Error: err}
						return
					}
					sinkChannel <- SinkData{nil, fetchRespMsg.Data, fetchRespMsg.Offset, fetchRespMsg.LastOffset}
				}
			case <-c.done:
				return
			case <-ctx.Done():
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
			c.Close()
			return nil, err
		}
		if err = c.client.Send(reqMsg); err != nil {
			c.Close()
			return nil, err
		}
		log.Println(123)
		return sinkChan, nil
	}

	return nil, errors.New("already subscribing")
}

func (c *Consumer) Connect(ctx context.Context, topicName string) error {
	if err := c.zkClient.Connect(); err != nil {
		return err
	}

	brokerAddrs, err := c.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		return err
	}
	if brokerAddrs == nil {
		return errors.New("topic doesn't exists")
	}
	// TODO:: Support partition for topic
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddrs[0], c.brokerPort)
	c.client = client.NewStreamClient(brokerEndpoint, paustqproto.SessionType_SUBSCRIBER)
	return c.client.Connect(ctx, topicName)
}

func (c *Consumer) Close() error {
	c.subscribing = false
	c.done <- true
	c.zkClient.Close()
	return c.client.Close()
}
