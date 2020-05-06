package producer

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
	"math/rand"
	"sync"
	"time"
)

type Producer struct {
	done          	chan bool
	doneWait      	chan bool
	client        	*client.StreamClient
	sourceChannel 	chan []byte
	publishing    	bool
	waitGroup     	sync.WaitGroup
	timeout      	 time.Duration
	chunkSize     	uint32
	zkClient    	*zookeeper.ZKClient
	brokerPort 		uint16
}

func NewProducer(zkHost string) *Producer {
	producer := &Producer{zkClient: zookeeper.NewZKClient(zkHost), publishing: false, chunkSize: 1024, brokerPort: common.DefaultBrokerPort}
	return producer
}

func (p *Producer) WithBrokerPort(port uint16) *Producer {
	p.brokerPort = port
	return p
}

func (p *Producer) WithTimeout(timeout time.Duration) *Producer {
	p.timeout = timeout
	return p
}

func (p *Producer) WithChunkSize(size uint32) *Producer {
	p.chunkSize = size
	return p
}
func (p *Producer) waitResponse(ctx context.Context, bChan chan bool) {

	for {
		select {
		case _, ok := <-bChan:
			if ok {
				msg, err := p.client.Receive()

				if err != nil {
					log.Fatal(err)
				} else if msg == nil {
					return
				} else {
					putRespMsg := &paustqproto.PutResponse{}
					if msg.UnpackTo(putRespMsg) != nil {
						log.Fatal("Failed to parse data to PutResponse")
					}
					p.waitGroup.Done()
				}
			} else {
				return
			}
		case <-p.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (p *Producer) startPublish(ctx context.Context) {

	if !p.publishing {
		return
	}

	p.sourceChannel = make(chan []byte)
	p.done = make(chan bool)

	waitResponseCh := make(chan bool, 5) // buffered channel to wait response
	go p.waitResponse(ctx, waitResponseCh)

	go func() {

		defer close(p.done)
		defer close(p.sourceChannel)
		defer close(waitResponseCh)

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

				waitResponseCh <- true

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

func (p *Producer) Connect(ctx context.Context, topicName string) error {
	if err := p.zkClient.Connect(); err != nil {
		return err
	}

	var brokerAddr string

	brokerHosts, err := p.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		return err
	}

	if brokerHosts == nil {
		brokers, err := p.zkClient.GetBrokers()
		if err != nil {
			return err
		}
		if brokers == nil {
			return errors.New("broker doesn't exists")
		}
		randBrokerIndex := rand.Intn(len(brokers))
		brokerAddr = brokers[randBrokerIndex]
	} else {
		brokerAddr = brokerHosts[0]
	}
	// TODO:: Support partition for topic
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddr, p.brokerPort)
	p.client = client.NewStreamClient(brokerEndpoint, paustqproto.SessionType_PUBLISHER)
	return p.client.Connect(ctx, topicName)
}

func (p *Producer) Close() error {
	p.publishing = false
	p.done <- true
	p.zkClient.Close()
	return p.client.Close()
}
