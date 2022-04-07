package client

import (
	"context"
	"errors"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"math/rand"
	"strconv"
)

type topicFragmentPair struct {
	topic      string
	fragmentId uint32
}

type PublishResult struct {
	Topic      string
	FragmentId uint32
	LastOffset uint64
}

type Producer struct {
	*client
	coordiWrapper      *coordinator_helper.CoordinatorWrapper
	config             *config.ProducerConfig
	publishTopics      []string
	logger             *logger.QLogger
	ctx                context.Context
	cancel             context.CancelFunc
	publishCh          chan *message.QMessage
	publishTargets     map[string][]*topicFragmentPair
	targetIndexCounter map[string]int
}

func NewProducer(config *config.ProducerConfig, publishTopics []string) *Producer {
	return NewProducerWithContext(context.Background(), config, publishTopics)
}

func NewProducerWithContext(ctx context.Context, config *config.ProducerConfig, publishTopics []string) *Producer {
	l := logger.NewQLogger("Producer", config.LogLevel())
	ctx, cancel := context.WithCancel(ctx)
	producer := &Producer{
		client:             newClient(config.ClientConfigBase),
		coordiWrapper:      coordinator_helper.NewCoordinatorWrapper(config.ServerAddresses(), uint(config.BootstrapTimeout()), 0, l),
		publishTopics:      publishTopics,
		logger:             l,
		ctx:                ctx,
		cancel:             cancel,
		publishCh:          make(chan *message.QMessage),
		publishTargets:     map[string][]*topicFragmentPair{},
		targetIndexCounter: map[string]int{},
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
	if err := p.coordiWrapper.Connect(); err != nil {
		return err
	}
	connectionTargets := make(map[string]*connectionTarget)

	for _, topic := range p.publishTopics {
		p.publishTargets[topic] = []*topicFragmentPair{}
		p.targetIndexCounter[topic] = 0
		// get all fragments : producer publishes data for all fragments of a topic
		fragments, err := p.coordiWrapper.GetTopicFragments(topic)
		if err != nil {
			return err
		}

		if len(fragments) == 0 {
			return pqerror.TopicFragmentNotExistsError{Topic: topic}
		}

		for _, fragment := range fragments {
			fid, err := strconv.ParseUint(fragment, 10, 32)
			if err != nil {
				return err
			}
			fragmentId := uint32(fid)
			p.publishTargets[topic] = append(p.publishTargets[topic], &topicFragmentPair{topic: topic, fragmentId: fragmentId})

			// get brokers from fragment
			addresses, err := p.coordiWrapper.GetBrokersOfTopic(topic, fragmentId)
			if err != nil {
				return pqerror.ZKOperateError{ErrStr: err.Error()}
			}
			// if any broker for topic-fragment doesn't exist, pick a random broker to publish
			if len(addresses) == 0 {
				brokerAddresses, err := p.coordiWrapper.GetBrokers()
				if err == nil && len(brokerAddresses) != 0 {
					brokerAddr := brokerAddresses[rand.Intn(len(brokerAddresses))]
					addresses = append(addresses, brokerAddr)
				} else {
					return pqerror.TopicFragmentBrokerNotExistsError{}
				}
			} else if len(addresses) > 1 {
				return pqerror.UnhandledError{ErrStr: "cannot handle replicated fragments: methods not implemented"}
			}

			// update connection target map
			for _, address := range addresses {
				if connectionTargets[address] == nil { // create single connection for each address
					topicFragment := common.NewTopic(topic, []uint32{fragmentId}, 0, 0)
					connectionTargets[address] = &connectionTarget{address: address, topics: []*common.Topic{topicFragment}}
				} else if topicFragments := connectionTargets[address].findTopicFragments(topic); topicFragments != nil { // append related fragment id for topic in connection
					topicFragments.AddFragmentId(fragmentId)
				} else { // if topic in connection-target doesn't exists, append new topicFragments to topics
					topicFragment := common.NewTopic(topic, []uint32{fragmentId}, 0, 0)
					connectionTargets[address].topics = append(connectionTargets[address].topics, topicFragment)
				}
			}
		}
	}

	var targets []*connectionTarget
	for _, target := range connectionTargets {
		targets = append(targets, target)
	}

	return p.connect(shapleqproto.SessionType_PUBLISHER, targets)
}

type PublishData struct {
	Topic  string
	Data   []byte
	SeqNum uint64
	NodeId string
}

func (p *Producer) getNextPublishTarget(topic string) *topicFragmentPair {
	defer func() {
		p.targetIndexCounter[topic] = (p.targetIndexCounter[topic] + 1) % len(p.publishTargets[topic])
	}()
	return p.publishTargets[topic][p.targetIndexCounter[topic]]
}

func (p *Producer) Publish(data *PublishData) (*PublishResult, error) {
	if _, exists := p.publishTargets[data.Topic]; !exists {
		return nil, pqerror.TopicNotSetError{}
	}

	pair := p.getNextPublishTarget(data.Topic)
	msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data.Data, data.SeqNum, data.NodeId, pair.topic, pair.fragmentId))
	if err != nil {
		return nil, err
	}

	conn := p.getConnections(pair.topic, pair.fragmentId)[0] // TODO:: when retrieve more than 1 address, replication factor should be handled
	if err := conn.socket.Write(msg); err != nil {
		return nil, err
	}

	res, err := conn.socket.Read()
	if err != nil {
		return nil, err
	}
	return p.handleMessage(res)
}

func (p *Producer) AsyncPublish(source <-chan *PublishData) (<-chan *PublishResult, <-chan error, error) {
	recvCh, recvErrCh, err := p.continuousReceive(p.ctx)
	errCh := make(chan error)

	if err != nil {
		return nil, nil, err
	}

	convertToQMsgCh := func(from <-chan *PublishData) <-chan *messageAndConnection {
		to := make(chan *messageAndConnection)
		go func() {
			defer close(to)
			for {
				select {
				case data, ok := <-from:
					if ok {
						pair := p.getNextPublishTarget(data.Topic)
						msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data.Data, data.SeqNum, data.NodeId, pair.topic, pair.fragmentId))
						if err != nil {
							errCh <- err
						} else {
							conn := p.getConnections(pair.topic, pair.fragmentId)[0] // TODO:: when retrieve more than 1 address, replication factor should be handled
							to <- &messageAndConnection{message: msg, connection: conn}
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
	sinkCh := make(chan *PublishResult)
	go func() {
		defer close(sinkCh)
		defer close(errCh)
		for {
			select {
			case <-p.ctx.Done():
				return
			case msg, ok := <-recvCh:
				if ok {
					fragment, err := p.handleMessage(msg.message)
					if err != nil {
						errCh <- err
					} else {
						sinkCh <- fragment
					}
				}
			}
		}
	}()

	return sinkCh, mergedErrCh, nil
}

func (p *Producer) Close() {
	p.cancel()
	close(p.publishCh)
	p.coordiWrapper.Close()
	p.close()
}

func (p *Producer) handleMessage(msg *message.QMessage) (*PublishResult, error) {
	if res, err := msg.UnpackTo(&shapleqproto.PutResponse{}); err == nil {
		putRes := res.(*shapleqproto.PutResponse)
		p.logger.Debug("received response - fragmentId: %d / offset: %d", putRes.FragmentId, putRes.LastOffset)
		return &PublishResult{Topic: putRes.TopicName, FragmentId: putRes.FragmentId, LastOffset: putRes.LastOffset}, nil
	} else if res, err := msg.UnpackTo(&shapleqproto.Ack{}); err == nil {
		return nil, errors.New(res.(*shapleqproto.Ack).GetMsg())
	} else {
		return nil, errors.New("received invalid type of message")
	}
}
