package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"log"
	"time"
)

type Consumer struct {
	ctx         context.Context
	client      *client.Client
	sinkChannel chan SinkData
	subscribing bool
}

type SinkData struct {
	Error error
	Data  []byte
}

func NewConsumer(ctx context.Context, hostUrl string, timeout time.Duration) *Consumer {
	c := client.NewClient(ctx, hostUrl, timeout, paustq_proto.SessionType_SUBSCRIBER)
	return &Consumer{ctx: ctx, client: c, sinkChannel: make(chan SinkData), subscribing: false}
}

func (c *Consumer) startSubscribe() {

	if !c.subscribing {
		return
	}
	onReceiveResponse := make(chan client.ReceivedData)

	for {
		go c.client.Read(onReceiveResponse, c.client.Timeout)

		select {
		case res := <-onReceiveResponse:
			if res.Error != nil {
				c.sinkChannel <- SinkData{res.Error, nil}
			} else {
				fetchRespMsg := &paustq_proto.FetchResponse{}
				if err := message.UnPackTo(res.Data, fetchRespMsg); err != nil {
					c.sinkChannel <- SinkData{err, nil}
					break
				}

				if fetchRespMsg.ErrorCode == 1 { // Consumed all record
					c.subscribing = false
					close(c.sinkChannel)
					return
				} else if fetchRespMsg.ErrorCode > 1 {
					c.sinkChannel <- SinkData{errors.New(fmt.Sprintf("FetchResponse Error: %d", fetchRespMsg.ErrorCode)), nil}
				} else {
					c.sinkChannel <- SinkData{nil, fetchRespMsg.Data}
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) Subscribe(topic string) chan SinkData {

	if c.subscribing == false {
		c.subscribing = true
		go c.startSubscribe()

		requestData, err := message.NewFetchRequestMsgData(0)
		if err != nil {
			log.Fatal("Failed to create FetchRequest message")
			return nil
		}

		err = c.client.Write(requestData)
		if err != nil {
			log.Fatal(err)
			return nil
		}
	}

	return c.sinkChannel
}

func (c *Consumer) Connect(topic string) error {
	return c.client.Connect(topic)
}

func (c *Consumer) Close() error {
	c.subscribing = false
	_, cancel := context.WithCancel(c.ctx)
	cancel()
	return c.client.Close()
}
