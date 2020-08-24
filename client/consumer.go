package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
)

type Consumer struct {
	*ClientBase
	config *config.ConsumerConfig
	topic  string
	logger *logger.QLogger
	ctx    context.Context
	cancel context.CancelFunc
}

func NewConsumer(config *config.ConsumerConfig, topic string) *Consumer {
	l := logger.NewQLogger("Consumer", config.LogLevel())
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		ClientBase: newClientBase(config.ClientConfigBase),
		config:     config,
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
	}
	return consumer
}

func NewConsumerWithContext(ctx context.Context, config *config.ConsumerConfig, topic string) *Consumer {
	l := logger.NewQLogger("Consumer", config.LogLevel())
	ctx, cancel := context.WithCancel(ctx)
	consumer := &Consumer{
		ClientBase: newClientBase(config.ClientConfigBase),
		config:     config,
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
	}
	return consumer
}

func (c Consumer) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

func (c *Consumer) Connect() error {
	return c.connect(shapleqproto.SessionType_SUBSCRIBER, c.topic)
}

func (c *Consumer) Subscribe(startOffset uint64) (<-chan FetchedData, <-chan error, error) {

	c.logger.Info("start subscribe.")

	reqMsg, err := message.NewQMessageFromMsg(message.STREAM, message.NewFetchRequestMsg(startOffset))
	if err != nil {
		return nil, nil, err
	}

	recvCh, sendCh, socketErrCh, err := c.continuousReadWrite()
	if err != nil {
		return nil, nil, err
	}
	sendCh <- reqMsg

	errCh := make(chan error)
	sinkCh := make(chan FetchedData)

	go func() {
		defer c.logger.Info("end subscribe")
		defer close(sinkCh)
		defer close(errCh)

		for {
			select {
			case <-c.ctx.Done():
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
			case msg, ok := <-recvCh:
				if ok {
					data, err := c.handleMessage(msg)
					if err != nil {
						errCh <- err
					} else {
						sinkCh <- FetchedData{data.Data, data.Offset, data.LastOffset}
					}
				} else {
					return
				}
			}
		}
	}()

	return sinkCh, errCh, nil
}

func (c *Consumer) Close() {
	c.cancel()
	c.close()
}

type FetchedData struct {
	Data               []byte
	Offset, LastOffset uint64
}

func (c *Consumer) handleMessage(msg *message.QMessage) (FetchedData, error) {
	if res, err := msg.UnpackAs(&shapleqproto.FetchResponse{}); err == nil {
		fetchRes := res.(*shapleqproto.FetchResponse)
		c.logger.Debug(fmt.Sprintf("received response - data : %s, last offset: %d, offset: %d",
			fetchRes.Data, fetchRes.LastOffset, fetchRes.Offset))
		return FetchedData{Data: fetchRes.Data, Offset: fetchRes.Offset, LastOffset: fetchRes.LastOffset}, nil
	} else if res, err := msg.UnpackAs(&shapleqproto.Ack{}); err == nil {
		return FetchedData{}, errors.New(res.(*shapleqproto.Ack).Msg)
	} else {
		return FetchedData{}, errors.New("received invalid type of message")
	}
}
