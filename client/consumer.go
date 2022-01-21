package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
)

type Consumer struct {
	*client
	config          *config.ConsumerConfig
	topic           string
	fragmentOffsets map[uint32]uint64
	logger          *logger.QLogger
	ctx             context.Context
	cancel          context.CancelFunc
	zkClient        *zookeeper.ZKQClient
}

func NewConsumer(config *config.ConsumerConfig, topic string, fragmentOffsets map[uint32]uint64) *Consumer {
	return NewConsumerWithContext(context.Background(), config, topic, fragmentOffsets)
}

func NewConsumerWithContext(ctx context.Context, config *config.ConsumerConfig, topic string, fragmentOffsets map[uint32]uint64) *Consumer {
	l := logger.NewQLogger("Consumer", config.LogLevel())
	zkClient := zookeeper.NewZKQClient(config.ServerAddresses(), uint(config.BootstrapTimeout()), 0)
	ctx, cancel := context.WithCancel(ctx)
	consumer := &Consumer{
		client:          newClient(config.ClientConfigBase, zkClient),
		config:          config,
		topic:           topic,
		fragmentOffsets: fragmentOffsets,
		logger:          l,
		ctx:             ctx,
		cancel:          cancel,
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
	if len(c.fragmentOffsets) == 0 {
		return pqerror.TopicFragmentNotExistsError{Topic: c.topic}
	}

	connectionTargets := make(map[string]*connectionTarget)
	for fragmentId := range c.fragmentOffsets {
		// get brokers from fragment
		addresses, err := c.zkClient.GetTopicFragmentBrokers(c.topic, fragmentId)
		if err != nil {
			return pqerror.ZKOperateError{ErrStr: err.Error()}
		}

		if len(addresses) == 0 {
			return pqerror.TopicFragmentBrokerNotExistsError{}
		} else if len(addresses) > 1 {
			return pqerror.UnhandledError{ErrStr: "cannot handle replicated fragments: methods not implemented"}
		}

		for _, address := range addresses {
			if connectionTargets[address] == nil { // create single connection for each address
				connectionTargets[address] = &connectionTarget{address: address, topic: c.topic, fragmentIds: []uint32{fragmentId}}
			} else { // append related fragment id for connection
				connectionTargets[address].fragmentIds = append(connectionTargets[address].fragmentIds, fragmentId)
			}
		}
	}
	var targets []*connectionTarget
	for _, target := range connectionTargets {
		targets = append(targets, target)
	}
	return c.connect(shapleqproto.SessionType_SUBSCRIBER, targets)
}

func (c *Consumer) Subscribe(maxBatchSize uint32, flushInterval uint32) (<-chan *SubscribeResult, <-chan error, error) {

	recvCh, recvErrCh, err := c.continuousReceive(c.ctx)
	if err != nil {
		return nil, nil, err
	}

	c.logger.Info("start subscribe.")

	sinkCh := make(chan *SubscribeResult)
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
					data, err := c.handleMessage(msg.message)
					if err != nil {
						errCh <- err
					} else {
						sinkCh <- data
					}
				}
			}
		}
	}()

	// send fetch request to every streams
	for _, conn := range c.connections {
		offsetsForConn := make(map[uint32]uint64) // make fragment-offset map for each connections
		for _, fragmentId := range conn.fragmentIds {
			offsetsForConn[fragmentId] = c.fragmentOffsets[fragmentId]
		}
		reqMsg, err := message.NewQMessageFromMsg(message.STREAM, message.NewFetchRequestMsg(offsetsForConn, maxBatchSize, flushInterval))
		if err != nil {
			return nil, nil, err
		}
		if err := conn.socket.Write(reqMsg); err != nil {
			return nil, nil, err
		}
	}

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

type SubscribeResult struct {
	Items      []*FetchedData
	LastOffset uint64
}

func (c *Consumer) handleMessage(msg *message.QMessage) (*SubscribeResult, error) {
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
		return &SubscribeResult{Items: []*FetchedData{fetched}, LastOffset: fetchRes.LastOffset}, nil

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
		return &SubscribeResult{Items: fetched, LastOffset: fetchRes.LastOffset}, nil

	} else if res, err := msg.UnpackTo(&shapleqproto.Ack{}); err == nil {
		return &SubscribeResult{}, errors.New(res.(*shapleqproto.Ack).Msg)
	} else {
		return &SubscribeResult{}, errors.New("received invalid type of message")
	}
}
