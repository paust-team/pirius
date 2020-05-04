package consumer

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/client"
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
}

type SinkData struct {
	Error              error
	Data               []byte
	Offset, LastOffset uint64
}

func NewConsumer(zkHost string) *Consumer {
	defaultZkClient := zookeeper.NewZKClient(zkHost)
	return &Consumer{zkClient: defaultZkClient, subscribing: false}
}

func (c *Consumer) WithTimeout(timeout time.Duration) *Consumer {
	c.timeout = timeout
	return c
}

func (c *Consumer) startSubscribe(ctx context.Context) chan SinkData {
	c.done = make(chan bool)
	sinkChannel := make(chan SinkData)

	go func() {
		defer close(c.done)
		defer close(sinkChannel)

		if !c.subscribing {
			return
		}

		onReceiveResponse := make(chan client.ReceivedData)

		for {
			go c.client.Receive(onReceiveResponse)

			select {
			case res := <-onReceiveResponse:
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

		return sinkChan, nil
	}

	return nil, errors.New("already subscribing")
}

func (c *Consumer) Connect(ctx context.Context, topicName string) error {
	if err := c.zkClient.Connect(); err != nil {
		return err
	}

	brokerHosts, err := c.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		return err
	}
	if brokerHosts == nil {
		return errors.New("topic doesn't exists")
	}
	// TODO:: Support partition for topic
	c.client = client.NewStreamClient(brokerHosts[0], paustqproto.SessionType_SUBSCRIBER)
	return c.client.Connect(ctx, topicName)
}

func (c *Consumer) Close() error {
	c.subscribing = false
	c.done <- true
	c.zkClient.Close()
	return c.client.Close()
}
