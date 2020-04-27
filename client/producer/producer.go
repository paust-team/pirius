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

type Producer struct {
	ctx           context.Context
	ctxCancel	  context.CancelFunc
	client        *client.StreamClient
	sourceChannel chan []byte
	publishing    bool
	waitGroup     sync.WaitGroup
	timeout       time.Duration
	chunkSize     uint32
}

func NewProducer(parentCtx context.Context, serverUrl string) *Producer {
	ctx, cancel := context.WithCancel(parentCtx)
	c := client.NewStreamClient(serverUrl, paustqproto.SessionType_PUBLISHER)
	producer := &Producer{ctx: ctx, ctxCancel: cancel, client: c, sourceChannel: make(chan []byte), publishing: false, chunkSize: 1024}
	return producer
}

func (p *Producer) WithTimeout(timeout time.Duration) *Producer {
	p.timeout = timeout
	return p
}

func (p *Producer) WithChunkSize(size uint32) *Producer {
	p.chunkSize = size
	return p
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

			reqMsg, err := message.NewQMessageFromMsg(message.NewPutRequestMsg(sourceData))
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
func (p *Producer) initPublish() {
	if p.publishing == false {
		p.publishing = true
		go p.startPublish()
	}
}
func (p *Producer) Publish(data []byte) {
	p.initPublish()
	p.waitGroup.Add(1)
	p.sourceChannel <- data
}

func (p *Producer) WaitAllPublishResponse() {
	if p.publishing {
		p.waitGroup.Wait()
	}
}

func (p *Producer) Connect(topic string) error {
	return p.client.ConnectWithTopic(p.ctx, topic)
}

func (p *Producer) Close() error {
	p.publishing = false
	p.ctxCancel()
	return p.client.Close()
}
