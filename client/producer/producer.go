package producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"math/rand"
	"sync"
	"time"
)

type Producer struct {
	done          	chan bool
	client        	*client.StreamClient
	sourceChannel 	chan []byte
	publishing    	bool
	waitGroup     	sync.WaitGroup
	timeout       	time.Duration
	chunkSize     	uint32
	zkClient      	*zookeeper.ZKClient
	brokerPort    	uint16
	logger 			*logger.QLogger
}

func NewProducer(zkHost string) *Producer {
	producer := &Producer{zkClient: zookeeper.NewZKClient(zkHost), publishing: false, chunkSize: 1024,
		brokerPort: common.DefaultBrokerPort, logger: logger.NewQLogger("Producer", logger.LogLevelInfo)}
	return producer
}

func (p *Producer) WithLogLevel(level logger.LogLevel) *Producer {
	p.logger.SetLogLevel(level)
	return p
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
func (p *Producer) waitPublished(ctx context.Context, bChan chan bool) {

	p.logger.Debug("start waiting publish response msg.")

	for {
		select {
		case _, ok := <-bChan:
			if ok {
				msg, err := p.client.Receive()

				if err != nil {
					p.logger.Error(err)
					return
				} else if msg == nil {
					p.logger.Debug("publish stream finished.")
					return
				} else {
					putRespMsg := &paustqproto.PutResponse{}
					if msg.UnpackTo(putRespMsg) != nil {
						p.logger.Error("Failed to unmarshal data to PutResponse")
						return
					}
					p.waitGroup.Done()
					p.logger.Debug("received publish response.")
				}
			} else {
				return
			}
		case <-p.done:
			p.logger.Debug("done channel closed. stop waitPublished")
			return
		case <-ctx.Done():
			p.logger.Debug("received ctx done. stop waitPublished")
			return
		}
	}
}

func (p *Producer) startPublish(ctx context.Context) {

	if !p.publishing {
		return
	}

	p.logger.Info("start publish.")

	p.sourceChannel = make(chan []byte)
	p.done = make(chan bool)

	waitResponseCh := make(chan bool, 5) // buffered channel to wait response
	go p.waitPublished(ctx, waitResponseCh)

	go func() {

		defer p.logger.Info("end publish")
		defer close(p.done)
		defer close(p.sourceChannel)
		defer close(waitResponseCh)

		for {
			select {
			case sourceData := <-p.sourceChannel:
				reqMsg, err := message.NewQMessageFromMsg(message.NewPutRequestMsg(sourceData))

				if err != nil {
					p.logger.Error(err)
					return
				}
				if err = p.client.Send(reqMsg); err != nil {
					p.logger.Error(err)
					return
				}
				p.logger.Debug("sent publish request msg.")

				waitResponseCh <- true

			case <-p.done:
				p.logger.Debug("done channel closed. stop publish")
				return
			case <-ctx.Done():
				p.logger.Debug("received ctx done. stop publish")
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
	p.logger.Debug("published data -> ", data)
}

func (p *Producer) WaitAllPublishResponse() {
	if p.publishing {
		p.waitGroup.Wait()
	}
	p.logger.Debug("wait all publish response done")
}

func (p *Producer) Connect(ctx context.Context, topicName string) error {

	if err := p.zkClient.Connect(); err != nil {
		p.logger.Error(err)
		return err
	}

	var brokerAddr string

	brokerHosts, err := p.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		p.logger.Error(err)
		return err
	}

	if brokerHosts == nil {
		brokers, err := p.zkClient.GetBrokers()
		if err != nil {
			p.logger.Error(err)
			return err
		}
		if brokers == nil {
			err = errors.New("broker doesn't exists")
			p.logger.Error(err)
			return err
		}
		randBrokerIndex := rand.Intn(len(brokers))
		brokerAddr = brokers[randBrokerIndex]
	} else {
		brokerAddr = brokerHosts[0]
	}
	// TODO:: Support partition for topic
	brokerEndpoint := fmt.Sprintf("%s:%d", brokerAddr, p.brokerPort)
	p.client = client.NewStreamClient(brokerEndpoint, paustqproto.SessionType_PUBLISHER)

	if err = p.client.Connect(ctx, topicName); err != nil {
		p.logger.Error(err)
		return err
	}

	p.logger.Info("producer is connected")
	return nil
}

func (p *Producer) Close() error {
	p.publishing = false
	p.done <- true
	p.zkClient.Close()

	if err := p.client.Close(); err != nil {
		p.logger.Error(err)
		return err
	}

	p.logger.Info("producer is closed")
	return nil
}
