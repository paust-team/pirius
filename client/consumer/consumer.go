package consumer

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"time"
)

type Consumer struct {
	done 			chan bool
	client       	*client.StreamClient
	subscribing  	bool
	timeout      	time.Duration
}

type SinkData struct {
	Error 				error
	Data  				[]byte
	Offset, LastOffset 	uint64
}

func NewConsumer(serverUrl string) *Consumer {
	c := client.NewStreamClient(serverUrl, paustqproto.SessionType_SUBSCRIBER)
	return &Consumer{client: c, subscribing: false}
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

func (c *Consumer) Connect(ctx context.Context, topic string) error {
	return c.client.Connect(ctx, topic)
}

func (c *Consumer) Close() error {
	c.subscribing = false
	c.done <- true
	return c.client.Close()
}
