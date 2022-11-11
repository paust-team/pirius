package rebalancing

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/broker/rebalancing/rule"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
	"sync"
)

type topicContext struct {
	ctx         context.Context
	cancelFn    context.CancelFunc
	option      topic.Option
	publishers  []string
	subscribers []string
}

type Rebalancer struct {
	bootstrapper  *bootstrapping.BootstrapService
	brokerHost    string
	running       bool
	masterNode    bool
	topicContexts sync.Map
	masterCtx     context.Context
	ruleExecutors []rule.Executor
	wg            sync.WaitGroup
}

func NewRebalancer(service *bootstrapping.BootstrapService, brokerHost string) Rebalancer {
	return Rebalancer{
		bootstrapper:  service,
		brokerHost:    brokerHost,
		running:       false,
		masterNode:    false,
		topicContexts: sync.Map{},
		ruleExecutors: []rule.Executor{
			rule.NewDefaultRuleExecutor(service),
			rule.NewDistributionRuleExecutor(service),
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

func (r *Rebalancer) dispatchRuleExecutor(option topic.Option) rule.Executor {
	for _, re := range r.ruleExecutors {
		switch v := re.(type) {
		case rule.DefaultRuleExecutor:
			if option == 0 {
				return v
			}
		case rule.DistributionRuleExecutor:
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
			r.topicContexts = sync.Map{}
			return err
		}
	}
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
	if !r.running || !r.masterNode {
		return qerror.InvalidStateError{State: fmt.Sprintf("running: %t / masterNode: %t", r.running, r.masterNode)}
	}
	if _, ok := r.topicContexts.Load(topic); ok {
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
	r.topicContexts.Store(topic, &topicContext{
		ctx:         topicCtx,
		cancelFn:    cancel,
		option:      topicFrame.Options(),
		publishers:  pubs,
		subscribers: subs,
	})

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
	if !r.running || !r.masterNode {
		return qerror.InvalidStateError{State: fmt.Sprintf("running: %t / masterNode: %t", r.running, r.masterNode)}
	}
	tc, ok := r.topicContexts.Load(topic)
	if !ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("watchers for topic(%s) not exist", topic)}
	}

	logger.Info("deregistering watchers", zap.String("topic", topic))
	tc.(*topicContext).cancelFn()

	r.topicContexts.Delete(topic)
	return nil
}

func (r *Rebalancer) rebalanceFragments(topicName string, publishers []string) error {
	tc, ok := r.topicContexts.Load(topicName)
	if !ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("context for topic(%s) not exist", topicName)}
	}

	rebalanceRuleExec := r.dispatchRuleExecutor(tc.(*topicContext).option)
	if rebalanceRuleExec == nil {
		return qerror.InvalidStateError{State: "cannot dispatch rule executor"}
	}

	if len(tc.(*topicContext).publishers) > len(publishers) {
		logger.Info("a publisher seems to have been removed",
			zap.Int("prev amount", len(tc.(*topicContext).publishers)),
			zap.Int("curr amount", len(publishers)))

		removedPublisher := helper.FindDiff(tc.(*topicContext).publishers, publishers)
		if removedPublisher == "" {
			return qerror.InvalidStateError{State: "no difference between old and new publishers"}
		}

		logger.Info("a removed publisher found", zap.String("publisher id", removedPublisher))
		if err := rebalanceRuleExec.OnPublisherRemoved(removedPublisher, topicName); err != nil {
			return err
		}

	} else {
		logger.Info("a publisher seems to have been added",
			zap.Int("prev amount", len(tc.(*topicContext).publishers)),
			zap.Int("curr amount", len(publishers)))

		addedPublisher := helper.FindDiff(publishers, tc.(*topicContext).publishers)
		if addedPublisher == "" {
			return qerror.InvalidStateError{State: "no difference between old and new publishers"}
		}

		publisherAddr, err := r.bootstrapper.GetPublisher(topicName, addedPublisher)
		if err != nil {
			return err
		}

		logger.Info("an added publisher found", zap.String("publisher id", addedPublisher), zap.String("address", publisherAddr))
		if err = rebalanceRuleExec.OnPublisherAdded(addedPublisher, topicName, publisherAddr); err != nil {
			return err
		}
	}

	tc.(*topicContext).publishers = publishers
	r.topicContexts.Store(topicName, tc)
	return nil
}

func (r *Rebalancer) rebalanceSubscriptions(topicName string, subscribers []string) error {
	tc, ok := r.topicContexts.Load(topicName)
	if !ok {
		return qerror.InvalidStateError{State: fmt.Sprintf("context for topic(%s) not exist", topicName)}
	}

	rebalanceRuleExec := r.dispatchRuleExecutor(tc.(*topicContext).option)
	if rebalanceRuleExec == nil {
		return qerror.InvalidStateError{State: "cannot dispatch rule executor"}
	}

	if len(tc.(*topicContext).subscribers) > len(subscribers) {
		logger.Info("a subscriber seems to have been removed",
			zap.Int("prev amount", len(tc.(*topicContext).publishers)),
			zap.Int("curr amount", len(subscribers)))

		removedSubscriber := helper.FindDiff(tc.(*topicContext).subscribers, subscribers)
		if removedSubscriber == "" {
			return qerror.InvalidStateError{State: "no difference between old and new subscribers"}
		}

		logger.Info("a removed subscriber found", zap.String("subscriber id", removedSubscriber))
		if err := rebalanceRuleExec.OnSubscriberRemoved(removedSubscriber, topicName); err != nil {
			return err
		}
	} else {
		logger.Info("a subscriber seems to have been added",
			zap.Int("prev amount", len(tc.(*topicContext).subscribers)),
			zap.Int("curr amount", len(subscribers)))

		addedSubscriber := helper.FindDiff(subscribers, tc.(*topicContext).subscribers)
		if addedSubscriber == "" {
			return qerror.InvalidStateError{State: "no difference between old and new publishers"}
		}

		logger.Info("an added subscriber found", zap.String("subscriber id", addedSubscriber))
		if err := rebalanceRuleExec.OnSubscriberAdded(addedSubscriber, topicName); err != nil {
			return err
		}
	}

	tc.(*topicContext).subscribers = subscribers
	r.topicContexts.Store(topicName, tc)

	return nil
}
