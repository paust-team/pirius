package client

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
)

type Producer struct {
	*ClientBase
	brokerAddr string
	topic      string
	logger     *logger.QLogger
	ctx        context.Context
	cancel     context.CancelFunc
	publishCh  chan *message.QMessage
}

func NewProducer(brokerAddr, topic string) *Producer {
	l := logger.NewQLogger("Producer", logger.Info)
	ctx, cancel := context.WithCancel(context.Background())
	producer := &Producer{
		ClientBase: newClientBase(),
		brokerAddr: brokerAddr,
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
		publishCh:  make(chan *message.QMessage),
	}
	return producer
}

func NewProducerWithContext(ctx context.Context, brokerAddr, topic string) *Producer {
	l := logger.NewQLogger("Producer", logger.Info)
	ctx, cancel := context.WithCancel(ctx)
	producer := &Producer{
		ClientBase: newClientBase(),
		brokerAddr: brokerAddr,
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
		publishCh:  make(chan *message.QMessage),
	}
	return producer
}

func (p *Producer) WithLogLevel(level logger.LogLevel) *Producer {
	p.logger.SetLogLevel(level)
	return p
}

func (p *Producer) WithTimeout(timeout uint) *Producer {
	p.setTimeout(timeout)
	return p
}

func (p Producer) Context() context.Context {
	if p.ctx != nil {
		return p.ctx
	}
	return context.Background()
}

func (p *Producer) Connect() error {
	return p.connect(paustqproto.SessionType_PUBLISHER, p.brokerAddr, p.topic)
}

func (p *Producer) Publish(data []byte) (common.Partition, error) {
	if err := p.send(message.NewQMessage(message.STREAM, data)); err != nil {
		return common.Partition{}, err
	}

	res, err := p.receive()
	if err != nil {
		return common.Partition{}, err
	}
	return p.handleMessage(res)
}

func (p *Producer) AsyncPublish(source <-chan []byte) (<-chan common.Partition, <-chan error, error) {
	recvCh, recvErrCh, err := p.continuousReceive(p.ctx)
	if err != nil {
		return nil, nil, err
	}

	convertToQMsgCh := func(from <-chan []byte) <-chan *message.QMessage {
		to := make(chan *message.QMessage)

		go func() {
			defer close(to)
			for data := range from {
				msgCh, _ := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data))
				to <- msgCh
			}
		}()

		return to
	}

	sendErrCh, err := p.continuousSend(p.ctx, convertToQMsgCh(source))
	if err != nil {
		return nil, nil, err
	}

	errCh := make(chan error)
	mergedErrCh := pqerror.MergeErrors(recvErrCh, sendErrCh, errCh)
	partitionCh := make(chan common.Partition)
	go func() {
		defer close(partitionCh)
		defer close(errCh)
		for {
			select {
			case <-p.ctx.Done():
				return
			case msg, ok := <-recvCh:
				if ok {
					partition, err := p.handleMessage(msg)
					if err != nil {
						errCh <- err
					} else {
						partitionCh <- partition
					}
				}

			}
		}
	}()

	return partitionCh, mergedErrCh, nil
}

func (p *Producer) Close() {
	p.cancel()
	close(p.publishCh)
	p.close()
}

func (p *Producer) handleMessage(msg *message.QMessage) (common.Partition, error) {
	if res, err := msg.UnpackAs(&paustqproto.PutResponse{}); err == nil {
		putRes := res.(*paustqproto.PutResponse)
		p.logger.Debug("received response - offset: %d", putRes.Partition.Offset)
		return common.Partition{Id: putRes.Partition.PartitionId, Offset: putRes.Partition.Offset}, nil
	} else if res, err := msg.UnpackAs(&paustqproto.Ack{}); err == nil {
		return common.Partition{}, errors.New(res.(*paustqproto.Ack).GetMsg())
	} else {
		return common.Partition{}, errors.New("received invalid type of message")
	}
}
