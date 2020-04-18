package producer

import (
	"context"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"log"
	"sync"
	"time"
)

type ResendableResponseData struct {
	requestData []byte
	responseCh  chan client.ReceivedData
}

type Producer struct {
	ctx           context.Context
	client        *client.StreamClient
	sourceChannel chan []byte
	publishing    bool
	waitGroup     sync.WaitGroup
}

func NewProducer(ctx context.Context, serverUrl string, timeout time.Duration) *Producer {
	c := client.NewStreamClient(ctx, serverUrl, paustqproto.SessionType_PUBLISHER)
	producer := &Producer{ctx: ctx, client: c, sourceChannel: make(chan []byte), publishing: false}
	return producer
}

func (p *Producer) waitResponse() {

	receiveChan := make(chan client.ReceivedData)
	go p.client.ReceiveToChan(receiveChan)

	select {
	case res := <-receiveChan:

		if res.Error != nil {
			log.Fatal(res.Error)
		} else if res.Msg == nil {
			p.waitGroup.Done()
			return
		} else {
			putRespMsg := &paustqproto.PutResponse{}
			if res.Msg.UnpackTo(putRespMsg) != nil {
				log.Fatal("Failed to parse data to PutResponse")
			}
			p.waitGroup.Done()
		}
	case <-p.ctx.Done():
		p.waitGroup.Done()
		return
	}
}

func (p *Producer) startPublish() {
	if !p.publishing {
		return
	}
	for {
		select {
		case sourceData := <-p.sourceChannel:

			reqMsg, err := message.NewQMessageWithPutRequest(sourceData)
			if err != nil {
				log.Fatal(err)
			}
			if err = p.client.Send(reqMsg); err != nil {
				log.Fatal(err)
			}
			go p.waitResponse()

		case <-p.ctx.Done():
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *Producer) Publish(data []byte) {
	if p.publishing == false {
		p.publishing = true
		go p.startPublish()
	}
	p.waitGroup.Add(1)
	p.sourceChannel <- data
}

func (p *Producer) WaitAllPublishResponse() {
	if p.publishing {
		p.waitGroup.Wait()
	}
}

func (p *Producer) Connect(topic string) error {
	return p.client.ConnectWithTopic(topic)
}

func (p *Producer) Close() error {
	p.publishing = false
	_, cancel := context.WithCancel(p.ctx)
	cancel()
	return p.client.Close()
}
