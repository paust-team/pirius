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
	"math/rand"
	"strconv"
)

type topicFragmentPair struct {
	topic      string
	fragmentId uint32
}

type PublishResult struct {
	FragmentId uint32
	LastOffset uint64
}

type Producer struct {
	*client
	zkqClient          *zookeeper.ZKQClient
	config             *config.ProducerConfig
	topic              string
	logger             *logger.QLogger
	ctx                context.Context
	cancel             context.CancelFunc
	publishCh          chan *message.QMessage
	publishTargets     []*topicFragmentPair
	currentTargetIndex int
}

func NewProducer(config *config.ProducerConfig, topic string) *Producer {
	return NewProducerWithContext(context.Background(), config, topic)
}

func NewProducerWithContext(ctx context.Context, config *config.ProducerConfig, topic string) *Producer {
	l := logger.NewQLogger("Producer", config.LogLevel())
	ctx, cancel := context.WithCancel(ctx)
	producer := &Producer{
		client:             newClient(config.ClientConfigBase),
		zkqClient:          zookeeper.NewZKQClient(config.ServerAddresses(), uint(config.BootstrapTimeout()), 0),
		topic:              topic,
		logger:             l,
		ctx:                ctx,
		cancel:             cancel,
		publishCh:          make(chan *message.QMessage),
		currentTargetIndex: 0,
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
	if err := p.zkqClient.Connect(); err != nil {
		return err
	}
	// get all fragments : producer publishes data for all fragments of a topic
	fragments, err := p.zkqClient.GetTopicFragments(p.topic)

	if err != nil {
		return err
	}

	if len(fragments) == 0 {
		return pqerror.TopicFragmentNotExistsError{Topic: p.topic}
	}

	connectionTargets := make(map[string]*connectionTarget)
	for _, fragment := range fragments {
		fid, err := strconv.ParseUint(fragment, 10, 32)
		if err != nil {
			return err
		}
		fragmentId := uint32(fid)
		p.publishTargets = append(p.publishTargets, &topicFragmentPair{topic: p.topic, fragmentId: fragmentId})

		// get brokers from fragment
		addresses, err := p.zkqClient.GetTopicFragmentBrokers(p.topic, fragmentId)
		if err != nil {
			return pqerror.ZKOperateError{ErrStr: err.Error()}
		}
		// if any broker for topic-fragment doesn't exist, pick a random broker to publish
		if len(addresses) == 0 {
			brokerAddresses, err := p.zkqClient.GetBrokers()
			if err == nil && len(brokerAddresses) != 0 {
				brokerAddr := brokerAddresses[rand.Intn(len(brokerAddresses))]
				addresses = append(addresses, brokerAddr)
			} else {
				return pqerror.TopicFragmentBrokerNotExistsError{}
			}
		} else if len(addresses) > 1 {
			return pqerror.UnhandledError{ErrStr: "cannot handle replicated fragments: methods not implemented"}
		}

		for _, address := range addresses {
			if connectionTargets[address] == nil { // create single connection for each address
				connectionTargets[address] = &connectionTarget{address: address, topic: p.topic, fragmentIds: []uint32{fragmentId}}
			} else { // append related fragment id for connection
				connectionTargets[address].fragmentIds = append(connectionTargets[address].fragmentIds, fragmentId)
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
	Data   []byte
	SeqNum uint64
	NodeId string
}

func (p *Producer) getNextPublishTarget() *topicFragmentPair {
	defer func() {
		p.currentTargetIndex = (p.currentTargetIndex + 1) % len(p.publishTargets)
	}()
	return p.publishTargets[p.currentTargetIndex]
}

func (p *Producer) Publish(data *PublishData) (*PublishResult, error) {

	pair := p.getNextPublishTarget()
	msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data.Data, data.SeqNum, data.NodeId, pair.fragmentId))
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
						pair := p.getNextPublishTarget()
						msg, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutRequestMsg(data.Data, data.SeqNum, data.NodeId, pair.fragmentId))
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
	p.zkqClient.Close()
	p.close()
}

func (p *Producer) handleMessage(msg *message.QMessage) (*PublishResult, error) {
	if res, err := msg.UnpackTo(&shapleqproto.PutResponse{}); err == nil {
		putRes := res.(*shapleqproto.PutResponse)
		p.logger.Debug("received response - fragmentId: %d / offset: %d", putRes.FragmentId, putRes.LastOffset)
		return &PublishResult{FragmentId: putRes.FragmentId, LastOffset: putRes.LastOffset}, nil
	} else if res, err := msg.UnpackTo(&shapleqproto.Ack{}); err == nil {
		return nil, errors.New(res.(*shapleqproto.Ack).GetMsg())
	} else {
		return nil, errors.New("received invalid type of message")
	}
}
