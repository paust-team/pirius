package producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/paustqpb"
	"time"
)

type Producer struct {
	client *client.Client
}

func NewProducer(hostUrl string, timeout time.Duration) *Producer {
	ctx := context.Background()
	c := client.NewClient(ctx, hostUrl, timeout)
	return &Producer{client: c}
}

func (p *Producer) Publish(topic string, inCh <- chan []byte, errCh chan <- error) {

	publishResChan := make(chan []byte)
	readErrChan := make(chan error)

	for {
		select {
		case data := <-inCh:

			protoMsg, protoErr := paustqpb.NewPutRequestMsg(topic, data)
			go p.client.ReadOne(publishResChan, readErrChan)

			if protoErr != nil {
				errCh <- protoErr
			} else {
				err := p.client.Write(protoMsg)
				if err != nil {
					errCh <- err
				}
			}

		case res := <-publishResChan:
			putRespMsg, err := paustqpb.ParseResponseMsg(res)
			if err != nil {
				errCh <- err
			} else if putRespMsg.ErrorCode != 0{
				errCh <- errors.New(fmt.Sprintf("PutResponse error: %d", putRespMsg.ErrorCode))
			}

		case readErr := <-readErrChan:
			errCh <- readErr

		case <- p.client.Ctx.Done():
			return
		}
	}
}

func (p *Producer) Connect() error {
	return p.client.Connect()
}

func (p *Producer) Close() error {
	return p.client.Close()
}