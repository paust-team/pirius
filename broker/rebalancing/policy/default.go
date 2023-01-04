package policy

import (
	"fmt"
	"github.com/paust-team/pirius/bootstrapping"
	"github.com/paust-team/pirius/bootstrapping/topic"
	"github.com/paust-team/pirius/constants"
	"github.com/paust-team/pirius/helper"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/qerror"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"
)

type Flushable interface {
	Flush() error
}
type Executor interface {
	OnPublisherAdded(id string, topicName string, host string) error
	OnPublisherRemoved(id string, topicName string) error
	OnSubscriberAdded(id string, topicName string) error
	OnSubscriberRemoved(id string, topicName string) error
}

type FlushableExecutor interface {
	Flushable
	Executor
}

type flusher struct {
	bootstrapper        *bootstrapping.BootstrapService
	mu                  sync.Mutex
	topicFragmentsMap   map[string]topic.FragMappingInfo
	subscriptionInfoMap map[string]topic.SubscriptionInfo
}

func (e *flusher) GetTopicFragmentMappings(topicName string) (topic.FragMappingInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.topicFragmentsMap[topicName] != nil {
		return e.topicFragmentsMap[topicName], nil
	}
	topicFragments, err := e.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return nil, err
	}

	return topicFragments.FragMappingInfo(), nil
}

func (e *flusher) UpdateTopicFragments(topicName string, fragMappings topic.FragMappingInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.topicFragmentsMap == nil {
		e.topicFragmentsMap = make(map[string]topic.FragMappingInfo)
	}
	e.topicFragmentsMap[topicName] = fragMappings
}

func (e *flusher) GetSubscriptionMappings(topicName string) (topic.SubscriptionInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.subscriptionInfoMap[topicName] != nil {
		return e.subscriptionInfoMap[topicName], nil
	}

	topicSubscriptions, err := e.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return nil, err
	}

	return topicSubscriptions.SubscriptionInfo(), nil
}

func (e *flusher) UpdateSubscriptionMappings(topicName string, subscriptionMappings topic.SubscriptionInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.subscriptionInfoMap == nil {
		e.subscriptionInfoMap = make(map[string]topic.SubscriptionInfo)
	}
	e.subscriptionInfoMap[topicName] = subscriptionMappings
}

// Flush : flush subscription info and fragment mappings into zk
// this is safe because only master broker do rebalance process
func (e *flusher) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.topicFragmentsMap != nil {
		for topicName, fragMappings := range e.topicFragmentsMap {
			topicFragmentFrame := topic.NewTopicFragmentsFrame(fragMappings)
			if err := e.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
				return err
			}
			logger.Info("updated fragment mappings flushed", zap.String("topic", topicName))
			logger.Debug(fmt.Sprintf("fragments-mappings: %#v\n", fragMappings))
			time.Sleep(100 * time.Millisecond) // for slowdown watch propagation
		}
	}
	if e.subscriptionInfoMap != nil {
		for topicName, subscriptionMappings := range e.subscriptionInfoMap {
			subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
			if err := e.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
				return err
			}
			logger.Info("updated subscription info flushed", zap.String("topic", topicName))
			logger.Debug(fmt.Sprintf("subscription-mappings: %#v\n", subscriptionMappings))
			time.Sleep(100 * time.Millisecond) // for slowdown watch propagation
		}
	}
	e.topicFragmentsMap = nil
	e.subscriptionInfoMap = nil
	return nil
}

type DefaultPolicyExecutor struct {
	Executor
	flusher
}

func NewDefaultPolicyExecutor(bootstrapper *bootstrapping.BootstrapService) *DefaultPolicyExecutor {
	return &DefaultPolicyExecutor{
		flusher: flusher{
			bootstrapper: bootstrapper,
			mu:           sync.Mutex{},
		},
	}
}

// OnPublisherAdded : when a publisher connects, it either sets the fragment active or adds a new one
// then add newly active fragments to each subscription equally
func (e *DefaultPolicyExecutor) OnPublisherAdded(id string, topicName string, host string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	fragMappings, err := e.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}
	pubsFragmentIds, err := getFragmentsToPublish(e.bootstrapper.CoordClientTopicWrapper, topicName, id, fragMappings)
	if err != nil {
		return err
	}

	for _, fragmentId := range pubsFragmentIds {
		fragMappings[fragmentId] = topic.FragInfo{
			State:       topic.Active,
			PublisherId: id,
			Address:     host,
		}
	}
	e.UpdateTopicFragments(topicName, fragMappings)
	logger.Info("update fragments to active state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	subscribers, err := e.bootstrapper.GetSubscribers(topicName)
	if err != nil {
		return err
	}

	subscriptionMappings, err := e.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}
	once := sync.Once{}
	var newSubsFragmentIds []uint

	for _, subscriberId := range subscribers {
		// in default rule, all subscribers must have same fragment assignment
		once.Do(func() {
			newSubsFragmentIds = subscriptionMappings[subscriberId]
			for _, fragmentId := range pubsFragmentIds { // include newly active pubs fragments
				if !helper.IsContains(fragmentId, subscriptionMappings[subscriberId]) {
					newSubsFragmentIds = append(newSubsFragmentIds, fragmentId)
				}
			}
		})
		subscriptionMappings[subscriberId] = newSubsFragmentIds
	}
	e.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnPublisherRemoved : when a publisher is disconnected, set fragment as inactive and remove fragments from subscriptions
func (e *DefaultPolicyExecutor) OnPublisherRemoved(id string, topicName string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	fragMappings, err := e.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}
	pubsFragmentIds, err := findPublisherFragments(e.bootstrapper.CoordClientTopicWrapper, topicName, id)
	if err != nil {
		return err
	}
	for _, fragmentId := range pubsFragmentIds {
		fragMappings[fragmentId] = topic.FragInfo{
			State:       topic.Inactive,
			PublisherId: id,
			Address:     "",
		}
	}
	e.UpdateTopicFragments(topicName, fragMappings)
	logger.Info("update fragments to inactive state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	subscriptionMappings, err := e.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}
	once := sync.Once{}
	var newSubsFragmentIds []uint

	for subscriberId, subsFragmentIds := range subscriptionMappings {
		// in default rule, all subscribers must have same fragment assignment
		once.Do(func() {
			for _, fragmentId := range subsFragmentIds {
				if !helper.IsContains(fragmentId, pubsFragmentIds) { // exclude inactive pubs fragments
					newSubsFragmentIds = append(newSubsFragmentIds, fragmentId)
				}
			}
		})
		subscriptionMappings[subscriberId] = newSubsFragmentIds
	}

	e.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnSubscriberAdded : when a subscriber connected, create new subscription for it and assign all fragments of topic.
func (e *DefaultPolicyExecutor) OnSubscriberAdded(id string, topicName string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	fragMappings, err := e.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}
	var allFragmentIds []uint
	for fragmentId, info := range fragMappings {
		if info.State == topic.Active {
			allFragmentIds = append(allFragmentIds, fragmentId)
		}
	}

	// update subscription info
	subscriptionMappings, err := e.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}

	if helper.HasAllElements(allFragmentIds, subscriptionMappings[id]) {
		logger.Info("skip assign subscription of subscriber",
			zap.String("topic", topicName),
			zap.String("subscriber", id),
			zap.Int("num-active-fragments", len(allFragmentIds)))
		return nil
	}

	subscriptionMappings[id] = allFragmentIds // assign new subscription for added subscriber
	e.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("add subscription of subscriber", zap.String("topic", topicName), zap.String("subscriber", id))
	return nil
}

// OnSubscriberRemoved : when a subscriber disconnected, just delete subscription of it.
func (e *DefaultPolicyExecutor) OnSubscriberRemoved(id string, topicName string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	subscriptionMappings, err := e.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}
	delete(subscriptionMappings, id) // remove a subscription of removed subscriber

	e.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("remove subscription of subscriber", zap.String("topic", topicName), zap.String("subscriber", id))
	return nil
}

// getAssignableFragmentId : find not assigned fragment id
func getAssignableFragmentId(topicName string, existingFragments topic.FragMappingInfo) (uint, error) {
	if len(existingFragments) == constants.MaxFragmentCount {
		return 0, qerror.InvalidStateError{State: fmt.Sprintf("fragment for topic(%s) is full", topicName)}
	}
	var newFragmentId uint
	for {
		newFragmentId = uint(rand.Intn(constants.MaxFragmentCount) + 1)
		if _, ok := existingFragments[newFragmentId]; !ok {
			break
		}
	}
	return newFragmentId, nil
}

// findPublisherFragments : find fragment info through topic-fragment discovery
func findPublisherFragments(topicCoord topic.CoordClientTopicWrapper, topicName string, publisherId string) (fragmentIds []uint, err error) {
	topicFragmentFrame, err := topicCoord.GetTopicFragments(topicName)
	if err != nil {
		return nil, err
	}
	fragMappings := topicFragmentFrame.FragMappingInfo()
	for fragId, fragInfo := range fragMappings {
		if fragInfo.PublisherId == publisherId {
			fragmentIds = append(fragmentIds, fragId)
		}
	}
	if len(fragmentIds) == 0 {
		return nil, qerror.TargetNotExistError{Target: fmt.Sprintf("fragments of topic(%s)", topicName)}
	}
	return
}

// getFragmentsToPublish : get fragments to publish for publisher
func getFragmentsToPublish(topicCoord topic.CoordClientTopicWrapper, topicName string, publisherId string, fragMappings topic.FragMappingInfo) (fragmentIds []uint, err error) {
	pubsFragmentIds, err := findPublisherFragments(topicCoord, topicName, publisherId)
	if err != nil {
		// if new publisher, assign new fragment id
		if _, ok := err.(qerror.TargetNotExistError); ok {
			newFragmentId, err := getAssignableFragmentId(topicName, fragMappings)
			if err != nil {
				return nil, err
			}
			pubsFragmentIds = []uint{newFragmentId}
		} else {
			return pubsFragmentIds, nil
		}
	}
	return pubsFragmentIds, nil
}
