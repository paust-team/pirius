package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/paustqpb"
	"log"
	"time"
)

type Consumer struct {
	client 			*client.Client
	sinkChannel		chan SinkData
	subscribing		bool
}

type SinkData struct {
	Error error
	Data []byte
}

func NewConsumer(hostUrl string, timeout time.Duration) *Consumer {
	ctx := context.Background()
	c := client.NewClient(ctx, hostUrl, timeout)
	return &Consumer{client: c, sinkChannel:make(chan SinkData), subscribing: false}
}

func (c *Consumer) startSubscribe() {

	onReceiveResponse := make(chan client.ResultData)

	for {
		go c.client.Read(onReceiveResponse)

		select {
		case res := <-onReceiveResponse:
			if res.Error != nil {
				c.sinkChannel <- SinkData{res.Error, nil}
				break
			}
			fetchRespMsg, err := paustqpb.ParseFetchResponseMsg(res.Data)
			if err != nil {
				c.sinkChannel <- SinkData{err, nil}
			} else if fetchRespMsg.ErrorCode != 0{
				c.sinkChannel <- SinkData{errors.New(fmt.Sprintf("FetchResponse Error: %d", fetchRespMsg.ErrorCode)), nil}
			} else {
				c.sinkChannel <- SinkData{err, fetchRespMsg.Data}
			}

		case <- c.client.Ctx.Done():
			return
		}
	}
}

func (c *Consumer) Subscribe(topic string) chan SinkData {

	if c.subscribing == false {
		c.subscribing = true

		protoMsg, protoErr := paustqpb.NewFetchRequestMsg(topic, 0)
		if protoErr != nil {
			log.Fatal("Error to creating FetchRequest message")
			return nil
		}

		err := c.client.Write(protoMsg)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		c.startSubscribe()
	}

	return c.sinkChannel
}

func (c *Consumer) Connect() error {
	return c.client.Connect()
}

func (c *Consumer) Close() error {
	c.subscribing = false
	return c.client.Close()
}