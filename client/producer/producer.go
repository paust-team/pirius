package producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"math/rand"
	"sync"
	"time"
)

type Producer struct {
	connected     bool
	mu            *sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	client        *client.StreamClient
	timeout       time.Duration
	chunkSize     uint32
	zkClient      *zookeeper.ZKClient
	brokerPort    uint16
	logger        *logger.QLogger
	bpMode        common.BackPressureMode
}

func NewProducer(zkHost string) *Producer {
	l := logger.NewQLogger("Producer", logger.Info)
	ctx, cancel := context.WithCancel(context.Background())

	producer := &Producer{
		connected: false,
		mu: &sync.Mutex{},
		ctx: 		ctx,
		cancel:		cancel,
		zkClient:   zookeeper.NewZKClient(zkHost),
		chunkSize:  1024,
		brokerPort: common.DefaultBrokerPort,
		logger:     l,
		bpMode:     common.AtMostOnce,
	}
	return producer
}

func (p *Producer) WithBackPressureMode(mode common.BackPressureMode) *Producer {
	p.bpMode = mode
	return p
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

func (p *Producer) Publish(ctx context.Context, sourceCh <- chan []byte) <- chan error {

	p.logger.Info("start publish.")

	errCh := make(chan error)

	go func() {

		defer p.logger.Info("end publish")
		defer close(errCh)

		doneRecvCh := make(chan bool)
		defer close(doneRecvCh)

		recvCh, err := p.client.ContinuousRead()
		if err != nil {
			p.logger.Error(err)
			errCh <- err
			return
		}

		msgHandler := message.Handler{}
		msgHandler.RegisterMsgHandle(&paustqproto.PutResponse{}, func(msg proto.Message) {
			res := msg.(*paustqproto.PutResponse)
			p.logger.Debug("received response: ", res)
		})
		msgHandler.RegisterMsgHandle(&paustqproto.Ack{}, func(msg proto.Message) {
			ack := msg.(*paustqproto.Ack)
			err := errors.New(fmt.Sprintf("received publish ack with error code %d", ack.Code))
			errCh <- err
			p.logger.Error(err)
		})

		for {
			select {
			case sourceData, ok := <- sourceCh:
				if !ok {
					return
				}
				reqMsg, err := message.NewQMessageFromMsg(message.NewPutRequestMsg(sourceData))

				if err != nil {
					p.logger.Error(err)
					errCh <- err
					return
				}
				if err = p.client.Send(reqMsg); err != nil {
					p.logger.Error(err)
					errCh <- err
					return
				}
				p.logger.Debug("sent publish request:", reqMsg)

			case msg := <- recvCh:
				if msg.Err != nil {
					var e pqerror.SocketClosedError
					if errors.As(err, &e) {
						p.logger.Debug("publish stream finished.")
						p.Close()
					} else {
						errCh <- err
					}
					return
				} else if err := msgHandler.Handle(msg.Msg); err != nil {
					errCh <- err
				}

			case <-p.ctx.Done():
				p.logger.Debug("producer closed. stop publish")
				return

			case <-ctx.Done():
				p.logger.Debug("received ctx done from parent. close producer")
				p.Close()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return errCh
}

func (p *Producer) Connect(ctx context.Context, topicName string) error {
	p.zkClient = p.zkClient.WithLogger(p.logger)
	if err := p.zkClient.Connect(); err != nil {
		p.logger.Error(err)
		return err
	}

	var brokerAddr string

	brokerHosts, err := p.zkClient.GetTopicBrokers(topicName)
	if err != nil {
		p.logger.Error(err)
		p.zkClient.Close()
		return err
	}

	if brokerHosts == nil {
		brokers, err := p.zkClient.GetBrokers()
		if err != nil {
			p.logger.Error(err)
			p.zkClient.Close()
			return err
		}
		if brokers == nil {
			err = errors.New("broker doesn't exists")
			p.logger.Error(err)
			p.zkClient.Close()
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
		p.zkClient.Close()
		return err
	}

	p.logger.Info("producer is connected")
	p.connected = true
	return nil
}

func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.connected {
		p.connected = false

		p.cancel()
		p.zkClient.Close()

		if err := p.client.Close(); err != nil {
			p.logger.Error(err)
			return err
		}

		p.logger.Info("producer is closed")
	}

	return nil
}
