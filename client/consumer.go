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
	"github.com/paust-team/shapleq/zookeeper"
)

type Consumer struct {
	*ClientBase
	config   *config.ConsumerConfig
	topic    string
	logger   *logger.QLogger
	ctx      context.Context
	cancel   context.CancelFunc
	zkClient *zookeeper.ZKQClient
}

func NewConsumer(config *config.ConsumerConfig, topic string) *Consumer {
	return NewConsumerWithContext(context.Background(), config, topic)
}

func NewConsumerWithContext(ctx context.Context, config *config.ConsumerConfig, topic string) *Consumer {
	l := logger.NewQLogger("Consumer", config.LogLevel())
	zkClient := zookeeper.NewZKQClient(config.ServerAddresses(), uint(config.BootstrapTimeout()), 0)
	ctx, cancel := context.WithCancel(ctx)
	consumer := &Consumer{
		ClientBase: newClientBase(config.ClientConfigBase, zkClient),
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

func (c *Consumer) Subscribe(startOffset uint64, maxBatchSize uint32, flushInterval uint32) (<-chan *FetchResult, <-chan error, error) {
	recvCh, recvErrCh, err := c.continuousReceive(c.ctx)
	if err != nil {
		return nil, nil, err
	}

	c.logger.Info("start subscribe.")

	reqMsg, err := message.NewQMessageFromMsg(message.STREAM, message.NewFetchRequestMsg(startOffset, maxBatchSize, flushInterval))
	if err != nil {
		return nil, nil, err
	}
	if err := c.send(reqMsg); err != nil {
		return nil, nil, err
	}

	sinkCh := make(chan *FetchResult)
	errCh := make(chan error)
	mergedErrCh := pqerror.MergeErrors(recvErrCh, errCh)
	go func() {
		defer c.logger.Info("end subscribe")
		defer close(sinkCh)
		defer close(errCh)

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
						sinkCh <- data
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
	Data           []byte
	Offset, SeqNum uint64
	NodeId         string
}

type FetchResult struct {
	Items      []*FetchedData
	LastOffset uint64
}

func (c *Consumer) handleMessage(msg *message.QMessage) (*FetchResult, error) {
	if res, err := msg.UnpackTo(&shapleqproto.FetchResponse{}); err == nil {
		fetchRes := res.(*shapleqproto.FetchResponse)
		c.logger.Debug(fmt.Sprintf("received response - data : %s, last offset: %d, offset: %d, seq num: %d, node id: %s",
			fetchRes.Data, fetchRes.LastOffset, fetchRes.Offset, fetchRes.SeqNum, fetchRes.NodeId))
		fetched := &FetchedData{
			Data:   fetchRes.Data,
			Offset: fetchRes.Offset,
			SeqNum: fetchRes.SeqNum,
			NodeId: fetchRes.NodeId,
		}
		return &FetchResult{Items: []*FetchedData{fetched}, LastOffset: fetchRes.LastOffset}, nil

	} else if res, err := msg.UnpackTo(&shapleqproto.BatchedFetchResponse{}); err == nil {
		fetchRes := res.(*shapleqproto.BatchedFetchResponse)
		c.logger.Debug(fmt.Sprintf("received response - data : %s, last offset: %d",
			fetchRes.Items, fetchRes.LastOffset))

		var fetched []*FetchedData
		for _, item := range fetchRes.Items {
			fetched = append(fetched, &FetchedData{
				Data:   item.Data,
				Offset: item.Offset,
				SeqNum: item.SeqNum,
				NodeId: item.NodeId,
			})
		}
		return &FetchResult{Items: fetched, LastOffset: fetchRes.LastOffset}, nil

	} else if res, err := msg.UnpackTo(&shapleqproto.Ack{}); err == nil {
		return &FetchResult{}, errors.New(res.(*shapleqproto.Ack).Msg)
	} else {
		return &FetchResult{}, errors.New("received invalid type of message")
	}
}
