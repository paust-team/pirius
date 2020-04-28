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
	done 		  chan bool
	doneWait	  chan bool
	client        *client.StreamClient
	sourceChannel chan []byte
	publishing    bool
	waitGroup     sync.WaitGroup
	timeout       time.Duration
	chunkSize     uint32
}

func NewProducer(serverUrl string) *Producer {
	c := client.NewStreamClient(serverUrl, paustqproto.SessionType_PUBLISHER)
	producer := &Producer{client: c, publishing: false, chunkSize: 1024}
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
func (p *Producer) waitResponse(ctx context.Context) {

	receiveChan := make(chan client.ReceivedData)
	go p.client.Receive(receiveChan)

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
	case <-p.doneWait:
		p.waitGroup.Done()
		return
	case <-ctx.Done():
		p.waitGroup.Done()
		return
	}
}

func (p *Producer) startPublish(ctx context.Context) {

	if !p.publishing {
		return
	}

	p.sourceChannel = make(chan []byte)
	p.done = make(chan bool)
	p.doneWait = make(chan bool)

	go func() {

		defer close(p.done)
		defer close(p.doneWait)
		defer close(p.sourceChannel)

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
				go p.waitResponse(ctx)

			case <-p.done:
				return
			case <-ctx.Done():
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

}

func (p *Producer) Publish(ctx context.Context, data []byte) {
	if p.publishing == false {
		p.publishing = true
		p.startPublish(ctx)
	}
	p.waitGroup.Add(1)
	p.sourceChannel <- data
}

func (p *Producer) WaitAllPublishResponse() {
	if p.publishing {
		p.waitGroup.Wait()
	}
}

func (p *Producer) Connect(ctx context.Context, topic string) error {
	return p.client.Connect(ctx, topic)
}

func (p *Producer) Close() error {
	p.publishing = false
	p.done <- true
	return p.client.Close()
}
