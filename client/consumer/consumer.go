package consumer

import (
	"context"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"log"
	"time"
)

type Consumer struct {
	ctx         	context.Context
	client        	*client.StreamClient
	sinkChannel 	chan SinkData
	subscribing 	bool
	endCondition	Condition
	timeout 		time.Duration
}

type SinkData struct {
	Error error
	Data  []byte
}

func NewConsumer(ctx context.Context, serverUrl string, endCondition Condition) *Consumer {
	c := client.NewStreamClient(ctx, serverUrl, paustqproto.SessionType_SUBSCRIBER)
	return &Consumer{ctx: ctx, client: c, sinkChannel: make(chan SinkData), subscribing: false, endCondition: endCondition}
}

func (c *Consumer) WithTimeout(timeout time.Duration) *Consumer {
	c.timeout = timeout
	return c
}

func (c *Consumer) startSubscribe() {

	if !c.subscribing {
		return
	}
	onReceiveResponse := make(chan client.ReceivedData)

	for {
		go c.client.ReceiveToChan(onReceiveResponse)

		select {
		case res := <-onReceiveResponse:
			if res.Error != nil {
				c.sinkChannel <- SinkData{res.Error, nil}
				close(c.sinkChannel)
				return
			} else if res.Msg == nil { // stream finished
				close(c.sinkChannel)
				return
			} else {
				fetchRespMsg := &paustqproto.FetchResponse{}
				if err := res.Msg.UnpackTo(fetchRespMsg); err != nil {
					c.sinkChannel <- SinkData{err, nil}
					return
				}

				c.sinkChannel <- SinkData{nil, fetchRespMsg.Data}
				if c.endCondition.Check(fetchRespMsg) {
					close(c.sinkChannel)
					return
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) Subscribe(startOffset uint64) chan SinkData {

	if c.subscribing == false {
		c.subscribing = true
		go c.startSubscribe()

		reqMsg, err := message.NewQMessageFromMsg(message.NewFetchRequestMsg(startOffset))
		if err != nil {
			log.Fatal(err)
		}
		if err = c.client.Send(reqMsg); err != nil {
			log.Fatal(err)
		}
	}

	return c.sinkChannel
}

func (c *Consumer) Connect(topic string) error {
	return c.client.ConnectWithTopic(topic)
}

func (c *Consumer) Close() error {
	c.subscribing = false
	_, cancel := context.WithCancel(c.ctx)
	cancel()
	return c.client.Close()
}
