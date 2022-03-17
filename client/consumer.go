package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
)

type Consumer struct {
	*client
	coordiWrapper *coordinator_helper.CoordinatorWrapper
	config        *config.ConsumerConfig
	topics        []*common.Topic
	logger        *logger.QLogger
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewConsumer(config *config.ConsumerConfig, topics []*common.Topic) *Consumer {
	return NewConsumerWithContext(context.Background(), config, topics)
}

func NewConsumerWithContext(ctx context.Context, config *config.ConsumerConfig, topics []*common.Topic) *Consumer {
	l := logger.NewQLogger("Consumer", config.LogLevel())
	ctx, cancel := context.WithCancel(ctx)
	consumer := &Consumer{
		client:        newClient(config.ClientConfigBase),
		coordiWrapper: coordinator_helper.NewCoordinatorWrapper(config.ServerAddresses(), uint(config.BootstrapTimeout()), 0, l),
		config:        config,
		topics:        topics,
		logger:        l,
		ctx:           ctx,
		cancel:        cancel,
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
	if err := c.coordiWrapper.Connect(); err != nil {
		return err
	}
	connectionTargets := make(map[string]*connectionTarget)

	for _, topicFragments := range c.topics {
		topic := topicFragments.TopicName()
		if len(topicFragments.FragmentIds()) == 0 {
			return pqerror.TopicFragmentNotExistsError{Topic: topic}
		}

		for fragmentId, startOffset := range topicFragments.FragmentOffsets() {
			// get brokers from fragment
			addresses, err := c.coordiWrapper.GetBrokersOfTopic(topic, fragmentId)
			if err != nil {
				return pqerror.ZKOperateError{ErrStr: err.Error()}
			}

			if len(addresses) == 0 {
				return pqerror.TopicFragmentBrokerNotExistsError{}
			} else if len(addresses) > 1 {
				return pqerror.UnhandledError{ErrStr: "cannot handle replicated fragments: methods not implemented"}
			}

			// update connection target map
			for _, address := range addresses {
				if connectionTargets[address] == nil { // create single connection for each address
					topicFragment := common.NewTopicFromFragmentOffsets(topic, common.FragmentOffsetMap{fragmentId: startOffset})
					connectionTargets[address] = &connectionTarget{address: address, topics: []*common.Topic{topicFragment}}
				} else if topicFragments := connectionTargets[address].findTopicFragments(topic); topicFragments != nil { // append related fragment id for topic in connection
					topicFragments.AddFragmentOffset(fragmentId, startOffset)
				} else { // if topic in connection-target doesn't exist, append new topicFragments to topics
					topicFragment := common.NewTopicFromFragmentOffsets(topic, common.FragmentOffsetMap{fragmentId: startOffset})
					connectionTargets[address].topics = append(connectionTargets[address].topics, topicFragment)
				}
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
		reqMsg, err := message.NewQMessageFromMsg(message.STREAM, message.NewFetchRequestMsg(conn.topics, maxBatchSize, flushInterval))
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
	c.coordiWrapper.Close()
	c.close()
}

type FetchedData struct {
	Data           []byte
	FragmentId     uint32
	Offset, SeqNum uint64
	NodeId         string
}

type SubscribeResult struct {
	Items     []*FetchedData
	TopicName string
}

func (c *Consumer) handleMessage(msg *message.QMessage) (*SubscribeResult, error) {
	if res, err := msg.UnpackTo(&shapleqproto.FetchResponse{}); err == nil {
		fetchRes := res.(*shapleqproto.FetchResponse)
		c.logger.Debug(fmt.Sprintf("received response - data : %s, offset: %d, seq num: %d, node id: %s",
			fetchRes.Data, fetchRes.Offset, fetchRes.SeqNum, fetchRes.NodeId))
		fetched := &FetchedData{
			Data:       fetchRes.Data,
			FragmentId: fetchRes.FragmentId,
			Offset:     fetchRes.Offset,
			SeqNum:     fetchRes.SeqNum,
			NodeId:     fetchRes.NodeId,
		}
		return &SubscribeResult{Items: []*FetchedData{fetched}, TopicName: fetchRes.TopicName}, nil

	} else if res, err := msg.UnpackTo(&shapleqproto.BatchedFetchResponse{}); err == nil {
		fetchRes := res.(*shapleqproto.BatchedFetchResponse)
		c.logger.Debug(fmt.Sprintf("received response - data : %s",
			fetchRes.Items))

		var fetched []*FetchedData
		for _, item := range fetchRes.Items {
			fetched = append(fetched, &FetchedData{
				Data:       item.Data,
				FragmentId: item.FragmentId,
				Offset:     item.Offset,
				SeqNum:     item.SeqNum,
				NodeId:     item.NodeId,
			})
		}
		return &SubscribeResult{Items: fetched, TopicName: fetchRes.TopicName}, nil

	} else if res, err := msg.UnpackTo(&shapleqproto.Ack{}); err == nil {
		return &SubscribeResult{}, errors.New(res.(*shapleqproto.Ack).Msg)
	} else {
		return &SubscribeResult{}, errors.New("received invalid type of message")
	}
}
