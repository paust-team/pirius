package client

import (
	"context"
	"errors"
	"fmt"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
)

type Consumer struct {
	*ClientBase
	brokerAddr string
	topic      string
	logger     *logger.QLogger
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewConsumer(brokerAddr, topic string) *Consumer {
	l := logger.NewQLogger("Consumer", logger.Info)
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		ClientBase: newClientBase(),
		brokerAddr: brokerAddr,
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
	}
	return consumer
}

func NewConsumerWithContext(ctx context.Context, brokerAddr, topic string) *Consumer {
	l := logger.NewQLogger("Consumer", logger.Info)
	ctx, cancel := context.WithCancel(ctx)
	consumer := &Consumer{
		ClientBase: newClientBase(),
		brokerAddr: brokerAddr,
		topic:      topic,
		logger:     l,
		ctx:        ctx,
		cancel:     cancel,
	}
	return consumer
}

func (c *Consumer) WithLogLevel(level logger.LogLevel) *Consumer {
	c.logger.SetLogLevel(level)
	return c
}

func (c *Consumer) WithTimeout(timeout uint) *Consumer {
	c.setTimeout(timeout)
	return c
}

func (c Consumer) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

func (c *Consumer) Connect() error {
	return c.connect(paustqproto.SessionType_SUBSCRIBER, c.brokerAddr, c.topic)
}

func (c *Consumer) Subscribe(startOffset uint64) (<-chan FetchedData, <-chan error, error) {
	recvCh, recvErrCh, err := c.continuousReceive(c.ctx)
	if err != nil {
		return nil, nil, err
	}

	c.logger.Info("start subscribe.")

	reqMsg, err := message.NewQMessageFromMsg(message.STREAM, message.NewFetchRequestMsg(startOffset))
	if err != nil {
		return nil, nil, err
	}
	if err := c.send(reqMsg); err != nil {
		return nil, nil, err
	}

	sinkCh := make(chan FetchedData)
	errCh := make(chan error)
	mergedErrCh := pqerror.MergeErrors(recvErrCh, errCh)
	go func() {
		defer c.logger.Info("end subscribe")
		defer close(sinkCh)
		defer close(errCh)

		if err != nil {
			c.logger.Error(err)
			errCh <- err
			return
		}

		for {
			select {
			case <-c.ctx.Done():
				return
			case msg, ok := <-recvCh:
				if ok {
					data, err := c.handleMessage(msg)
					if err != nil {
						errCh <- err
					} else {
						sinkCh <- FetchedData{data.Data, data.Offset, data.LastOffset}
					}
				}
			}
		}
	}()

	return sinkCh, mergedErrCh, nil
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
	if res, err := msg.UnpackAs(&paustqproto.FetchResponse{}); err == nil {
		fetchRes := res.(*paustqproto.FetchResponse)
		c.logger.Debug(fmt.Sprintf("received response - data : %s, last offset: %d, offset: %d",
			fetchRes.Data, fetchRes.LastOffset, fetchRes.Offset))
		return FetchedData{Data: fetchRes.Data, Offset: fetchRes.Offset, LastOffset: fetchRes.LastOffset}, nil
	} else if res, err := msg.UnpackAs(&paustqproto.Ack{}); err == nil {
		return FetchedData{}, errors.New(res.(*paustqproto.Ack).Msg)
	} else {
		return FetchedData{}, errors.New("received invalid type of message")
	}
}
