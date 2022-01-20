package client

import (
	"context"
	"errors"
	"github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
)

type Producer struct {
	*ClientBase
	config    *config.ProducerConfig
	topic     string
	logger    *logger.QLogger
	ctx       context.Context
	cancel    context.CancelFunc
	publishCh chan *message.QMessage
}

func NewProducer(config *config.ProducerConfig, topic string) *Producer {
	return NewProducerWithContext(context.Background(), config, topic)
}

func NewProducerWithContext(ctx context.Context, config *config.ProducerConfig, topic string) *Producer {
	l := logger.NewQLogger("Producer", config.LogLevel())
	zkClient := zookeeper.NewZKQClient(config.ServerAddresses(), uint(config.BootstrapTimeout()), 0)
	ctx, cancel := context.WithCancel(ctx)
	producer := &Producer{
		ClientBase: newClientBase(config.ClientConfigBase, zkClient),
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
		publishCh:  make(chan *message.QMessage),
	}
	return producer
}

func (p Producer) Context() context.Context {
	if p.ctx != nil {
		return p.ctx
	}
	return context.Background()
}

func (p *Producer) Connect() error {
	return p.connect(shapleqproto.SessionType_PUBLISHER, p.topic)
}

type PublishData struct {
	Data   []byte
	SeqNum uint64
	NodeId string
}

func (p *Producer) Publish(data *PublishData) (*shapleqproto.Fragment, error) {
	msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data.Data, data.SeqNum, data.NodeId))
	if err != nil {
		return nil, err
	}

	if err := p.send(msg); err != nil {
		return nil, err
	}

	res, err := p.receive()
	if err != nil {
		return nil, err
	}
	return p.handleMessage(res)
}

func (p *Producer) AsyncPublish(source <-chan *PublishData) (<-chan *shapleqproto.Fragment, <-chan error, error) {
	recvCh, recvErrCh, err := p.continuousReceive(p.ctx)
	errCh := make(chan error)

	if err != nil {
		return nil, nil, err
	}

	convertToQMsgCh := func(from <-chan *PublishData) <-chan *message.QMessage {
		to := make(chan *message.QMessage)
		go func() {
			defer close(to)
			for {
				select {
				case data, ok := <-from:
					if ok {
						msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data.Data, data.SeqNum, data.NodeId))
						if err != nil {
							errCh <- err
						} else {
							to <- msg
						}
					}
				case <-p.ctx.Done():
					return
				}
			}
		}()

		return to
	}

	sendErrCh, err := p.continuousSend(p.ctx, convertToQMsgCh(source))
	if err != nil {
		return nil, nil, err
	}

	mergedErrCh := pqerror.MergeErrors(recvErrCh, sendErrCh, errCh)
	fragmentCh := make(chan *shapleqproto.Fragment)
	go func() {
		defer close(fragmentCh)
		defer close(errCh)
		for {
			select {
			case <-p.ctx.Done():
				return
			case msg, ok := <-recvCh:
				if ok {
					fragment, err := p.handleMessage(msg)
					if err != nil {
						errCh <- err
					} else {
						fragmentCh <- fragment
					}
				}
			}
		}
	}()

	return fragmentCh, mergedErrCh, nil
}

func (p *Producer) Close() {
	p.cancel()
	close(p.publishCh)
	p.close()
}

func (p *Producer) handleMessage(msg *message.QMessage) (*shapleqproto.Fragment, error) {
	if res, err := msg.UnpackTo(&shapleqproto.PutResponse{}); err == nil {
		putRes := res.(*shapleqproto.PutResponse)
		p.logger.Debug("received response - offset: %d", putRes.Fragment.LastOffset)
		return putRes.Fragment, nil
	} else if res, err := msg.UnpackTo(&shapleqproto.Ack{}); err == nil {
		return nil, errors.New(res.(*shapleqproto.Ack).GetMsg())
	} else {
		return nil, errors.New("received invalid type of message")
	}
}
