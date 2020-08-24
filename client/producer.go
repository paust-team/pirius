package client

import (
	"context"
	"errors"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
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
	l := logger.NewQLogger("Producer", logger.Info)
	ctx, cancel := context.WithCancel(context.Background())
	producer := &Producer{
		ClientBase: newClientBase(config.ClientConfigBase),
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
	}
	return producer
}

func NewProducerWithContext(ctx context.Context, config *config.ProducerConfig, topic string) *Producer {
	l := logger.NewQLogger("Producer", logger.Info)
	ctx, cancel := context.WithCancel(ctx)
	producer := &Producer{
		ClientBase: newClientBase(config.ClientConfigBase),
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
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

func (p *Producer) Publish(data []byte) (common.Partition, error) {
	msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data))
	if err != nil {
		return common.Partition{}, err
	}

	if err := p.send(msg); err != nil {
		return common.Partition{}, err
	}

	res, err := p.receive()
	if err != nil {
		return common.Partition{}, err
	}
	return p.handleMessage(res)
}

func (p *Producer) AsyncPublish(source <-chan []byte) (<-chan common.Partition, <-chan error, error) {

	recvCh, sendCh, socketErrCh, err := p.continuousReadWrite()
	if err != nil {
		return nil, nil, err
	}

	errCh := make(chan error)
	partitionCh := make(chan common.Partition)

	go func() {
		defer p.logger.Info("end async publish")
		defer close(partitionCh)
		defer close(errCh)
		for {
			select {
			case <-p.ctx.Done():
				return
			case err, ok := <-socketErrCh:
				if ok {
					if pqErr, ok := err.(pqerror.PQError); ok {
						switch pqErr.(type) {
						case pqerror.SocketClosedError:
							return
						case pqerror.BrokenPipeError:
							return
						case pqerror.ConnResetError:
							return
						}
					}
					errCh <- err
				}

			case data, ok := <-source:
				if ok {
					msg, _ := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data))
					sendCh <- msg
				}
			case msg, ok := <-recvCh:
				if ok {
					partition, err := p.handleMessage(msg)
					if err != nil {
						errCh <- err
					} else {
						partitionCh <- partition
					}
				} else {
					return
				}
			}
		}
	}()

	return partitionCh, errCh, nil
}

func (p *Producer) Close() {
	p.cancel()
	p.close()
}

func (p *Producer) handleMessage(msg *message.QMessage) (common.Partition, error) {
	if res, err := msg.UnpackAs(&shapleqproto.PutResponse{}); err == nil {
		putRes := res.(*shapleqproto.PutResponse)
		p.logger.Debug("received response - offset: %d", putRes.Partition.Offset)
		return common.Partition{Id: putRes.Partition.PartitionId, Offset: putRes.Partition.Offset}, nil
	} else if res, err := msg.UnpackAs(&shapleqproto.Ack{}); err == nil {
		return common.Partition{}, errors.New(res.(*shapleqproto.Ack).GetMsg())
	} else {
		return common.Partition{}, errors.New("received invalid type of message")
	}
}
