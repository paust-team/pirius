package rebalancing

import (
	"context"
	"fmt"
	"github.com/paust-team/pirius/bootstrapping"
	"github.com/paust-team/pirius/bootstrapping/topic"
	"github.com/paust-team/pirius/broker/rebalancing/policy"
	"github.com/paust-team/pirius/helper"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/qerror"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"
)

type topicContext struct {
	ctx         context.Context
	cancelFn    context.CancelFunc
	option      topic.Option
	publishers  []string
	subscribers []string
}

type Rebalancer struct {
	bootstrapper    *bootstrapping.BootstrapService
	brokerHost      string
	running         bool
	masterNode      bool
	topicContexts   map[string]*topicContext
	masterCtx       context.Context
	policyExecutors []policy.FlushableExecutor
	wg              sync.WaitGroup
	mu              sync.Mutex
}

func NewRebalancer(service *bootstrapping.BootstrapService, brokerHost string) Rebalancer {
	rand.Seed(time.Now().UnixNano())

	return Rebalancer{
		bootstrapper:  service,
		brokerHost:    brokerHost,
		running:       false,
		masterNode:    false,
		topicContexts: make(map[string]*topicContext),
		policyExecutors: []policy.FlushableExecutor{
			policy.NewDefaultPolicyExecutor(service),
			policy.NewDistributionPolicyExecutor(service),
		},
		wg: sync.WaitGroup{},
	}
}

func (r *Rebalancer) Run(ctx context.Context) error {
	if r.running {
		return qerror.InvalidStateError{State: "already running"}
	}
	brokers, err := r.bootstrapper.GetBrokers()
	if err != nil {
		return err
	}
	r.running = true

	// if this node is not master, wait until be master
	isMaster, err := r.checkMasterNode(brokers)
	if err != nil {
		return err
	}
	if !isMaster {
		if err = r.waitUntilBeMaster(ctx); err != nil {
			r.running = false
			return err
		}
	} else { // register pubs/subs paths for triggering rebalance process
		r.masterNode = true
		if err = r.prepareRebalance(ctx); err != nil {
			r.running = false
			return err
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			r.running = false
			return
		}
	}()
	return nil
}

func (r *Rebalancer) Wait() {
	r.wg.Wait()
}
func (r *Rebalancer) IsRunning() bool {
	return r.running
}

func (r *Rebalancer) IsMasterNode() bool {
	return r.masterNode
}

func (r *Rebalancer) dispatchPolicyExecutor(option topic.Option) policy.FlushableExecutor {
	for _, re := range r.policyExecutors {
		switch v := re.(type) {
		case *policy.DefaultPolicyExecutor:
			if option == 0 {
				return v
			}
		case *policy.DistributionPolicyExecutor:
			if option&topic.UniquePerFragment != 0 {
				return v
			}
		default:
		}
	}
	return nil
}

func (r *Rebalancer) checkMasterNode(brokers []string) (bool, error) {
	masterHost, err := r.bootstrapper.GetBroker(brokers[0])
	if err != nil {
		return false, err
	}
	return masterHost == r.brokerHost, nil
}

func (r *Rebalancer) prepareRebalance(ctx context.Context) error {
	masterCtx, cancel := context.WithCancel(ctx)
	r.masterCtx = masterCtx
	topics, err := r.bootstrapper.GetTopics()
	if err != nil {
		cancel()
		return err
	}

	logger.Info("registering watchers", zap.Strings("topics", topics))
	for _, t := range topics {
		if err = r.RegisterTopicWatchers(t); err != nil {
			cancel()
			r.topicContexts = make(map[string]*topicContext)
			return err
		}
	}
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
		}
	}()
	return nil
}

func (r *Rebalancer) waitUntilBeMaster(ctx context.Context) error {
	innerCtx, cancel := context.WithCancel(ctx)
	brokersCh, err := r.bootstrapper.WatchBrokersPathChanged(innerCtx)
	if err != nil {
		cancel()
		return err
	}
	logger.Info("waiting to be a master node")
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer cancel()
		for changedBrokers := range brokersCh {
			isMaster, err := r.checkMasterNode(changedBrokers)
			if err != nil {
				logger.Error(err.Error())
				return
			}
			if isMaster {
				r.masterNode = true
				if err = r.prepareRebalance(ctx); err != nil {
					logger.Error(err.Error())
				}
				return
			}
		}
	}()
	return nil
}

func (r *Rebalancer) RegisterTopicWatchers(topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.running || !r.masterNode {
		return qerror.InvalidStateError{State: fmt.Sprintf("running: %t / masterNode: %t", r.running, r.masterNode)}
	}
	if _, ok := r.topicContexts[topic]; ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("watchers for topic(%s) already exist", topic)}
	}

	topicFrame, err := r.bootstrapper.GetTopic(topic)
	if err != nil {
		return err
	}

	pubs, err := r.bootstrapper.GetPublishers(topic)
	if err != nil {
		return err
	}
	subs, err := r.bootstrapper.GetSubscribers(topic)
	if err != nil {
		return err
	}

	topicCtx, cancel := context.WithCancel(r.masterCtx)
	r.topicContexts[topic] = &topicContext{
		ctx:         topicCtx,
		cancelFn:    cancel,
		option:      topicFrame.Options(),
		publishers:  pubs,
		subscribers: subs,
	}

	pubsCh, err := r.bootstrapper.WatchPubsPathChanged(topicCtx, topic)
	if err != nil {
		cancel()
		return err
	}

	subsCh, err := r.bootstrapper.WatchSubsPathChanged(topicCtx, topic)
	if err != nil {
		cancel()
		return err
	}

	logger.Info("watchers are registered", zap.String("topic", topic))
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer logger.Info("watchers are de-registered", zap.String("topic", topic))
		defer cancel()
		for {
			select {
			case updatedPubs, ok := <-pubsCh:
				if !ok {
					return
				}
				if err = r.rebalanceFragments(topic, updatedPubs); err != nil {
					logger.Error(err.Error())
				}
			case updatedSubs, ok := <-subsCh:
				if !ok {
					return
				}
				if err = r.rebalanceSubscriptions(topic, updatedSubs); err != nil {
					logger.Error(err.Error())
				}
			}
		}
	}()

	return nil
}

func (r *Rebalancer) DeregisterTopicWatchers(topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.running || !r.masterNode {
		return qerror.InvalidStateError{State: fmt.Sprintf("running: %t / masterNode: %t", r.running, r.masterNode)}
	}
	tc, ok := r.topicContexts[topic]
	if !ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("watchers for topic(%s) not exist", topic)}
	}

	logger.Info("deregistering watchers", zap.String("topic", topic))
	tc.cancelFn()

	delete(r.topicContexts, topic)
	return nil
}

func (r *Rebalancer) rebalanceFragments(topicName string, publishers []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	tc, ok := r.topicContexts[topicName]
	if !ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("context for topic(%s) not exist", topicName)}
	}

	rebalancePolicyExec := r.dispatchPolicyExecutor(tc.option)
	if rebalancePolicyExec == nil {
		return qerror.InvalidStateError{State: "cannot dispatch policy executor"}
	}

	var addedPublishers, removedPublishers []string
	if len(tc.publishers) != len(publishers) || !helper.HasAllElements(tc.publishers, publishers) {
		addedPublishers = helper.FindDiff(publishers, tc.publishers)
		removedPublishers = helper.FindDiff(tc.publishers, publishers)
	} else {
		return qerror.InvalidStateError{State: "no difference between old and new publishers"}
	}

	if len(removedPublishers) > 0 {
		logger.Info("few publishers seems to have been removed",
			zap.Int("prev amount", len(tc.publishers)),
			zap.Int("removed amount", len(removedPublishers)))

		for _, removedPublisher := range removedPublishers {
			logger.Info("a removed publisher found", zap.String("publisher id", removedPublisher))
			if err := rebalancePolicyExec.OnPublisherRemoved(removedPublisher, topicName); err != nil {
				return err
			}
		}
	}
	if len(addedPublishers) > 0 {
		logger.Info("few publishers seems to have been added",
			zap.Int("prev amount", len(tc.publishers)),
			zap.Int("added amount", len(addedPublishers)))

		for _, addedPublisher := range addedPublishers {
			publisherAddr, err := r.bootstrapper.GetPublisher(topicName, addedPublisher)
			if err != nil {
				return err
			}
			logger.Info("an added publisher found", zap.String("publisher id", addedPublisher), zap.String("address", publisherAddr))
			if err = rebalancePolicyExec.OnPublisherAdded(addedPublisher, topicName, publisherAddr); err != nil {
				return err
			}
		}
	}
	if err := rebalancePolicyExec.Flush(); err != nil {
		return err
	}
	tc.publishers = publishers
	return nil
}

func (r *Rebalancer) rebalanceSubscriptions(topicName string, subscribers []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	tc, ok := r.topicContexts[topicName]
	if !ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("context for topic(%s) not exist", topicName)}
	}

	rebalancePolicyExec := r.dispatchPolicyExecutor(tc.option)
	if rebalancePolicyExec == nil {
		return qerror.InvalidStateError{State: "cannot dispatch policy executor"}
	}

	var addedSubscribers, removedSubscribers []string
	if len(tc.subscribers) != len(subscribers) || !helper.HasAllElements(tc.subscribers, subscribers) {
		addedSubscribers = helper.FindDiff(subscribers, tc.subscribers)
		removedSubscribers = helper.FindDiff(tc.subscribers, subscribers)
	} else {
		return qerror.InvalidStateError{State: "no difference between old and new subscribers"}
	}

	if len(removedSubscribers) > 0 {
		logger.Info("few subscribers seems to have been removed",
			zap.Int("prev amount", len(tc.publishers)),
			zap.Int("removed amount", len(removedSubscribers)))

		for _, removedSubscriber := range removedSubscribers {
			logger.Info("a removed subscriber found", zap.String("subscriber id", removedSubscriber))
			if err := rebalancePolicyExec.OnSubscriberRemoved(removedSubscriber, topicName); err != nil {
				return err
			}
		}
	}
	if len(addedSubscribers) > 0 {
		logger.Info("few subscribers seems to have been added",
			zap.Int("prev amount", len(tc.subscribers)),
			zap.Int("added amount", len(addedSubscribers)))

		for _, addedSubscriber := range addedSubscribers {
			logger.Info("an added subscriber found", zap.String("subscriber id", addedSubscriber))
			if err := rebalancePolicyExec.OnSubscriberAdded(addedSubscriber, topicName); err != nil {
				return err
			}
		}
	}

	if err := rebalancePolicyExec.Flush(); err != nil {
		return err
	}
	tc.subscribers = subscribers

	return nil
}
