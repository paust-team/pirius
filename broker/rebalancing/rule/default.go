package rule

import (
	"fmt"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
	"math/rand"
	"sync"
)

type Executor interface {
	OnPublisherAdded(id string, topicName string, host string) error
	OnPublisherRemoved(id string, topicName string) error
	OnSubscriberAdded(id string, topicName string) error
	OnSubscriberRemoved(id string, topicName string) error
}

type DefaultRuleExecutor struct {
	bootstrapper *bootstrapping.BootstrapService
}

func NewDefaultRuleExecutor(bootstrapper *bootstrapping.BootstrapService) DefaultRuleExecutor {
	return DefaultRuleExecutor{bootstrapper: bootstrapper}
}

// OnPublisherAdded : when a publisher connects, it either sets the fragment active or adds a new one
// then add newly active fragments to each subscription equally
func (e DefaultRuleExecutor) OnPublisherAdded(id string, topicName string, host string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicFragments, err := e.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	fragMappings := topicFragments.FragMappingInfo()

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
	topicFragmentFrame := topic.NewTopicFragmentsFrame(fragMappings)
	if err = e.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
		return err
	}
	logger.Info("update fragments to active state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	topicSubscriptions, err := e.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()
	once := sync.Once{}
	var newSubsFragmentIds []uint

	for subscriberId, subsFragmentIds := range subscriptionMappings {
		// in default rule, all subscribers must have same fragment assignment
		once.Do(func() {
			newSubsFragmentIds = subsFragmentIds
			for _, fragmentId := range pubsFragmentIds { // include newly active pubs fragments
				if !helper.IsContains(fragmentId, subsFragmentIds) {
					newSubsFragmentIds = append(newSubsFragmentIds, fragmentId)
				}
			}
		})
		subscriptionMappings[subscriberId] = newSubsFragmentIds
	}
	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = e.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnPublisherRemoved : when a publisher is disconnected, set fragment as inactive and remove fragments from subscriptions
func (e DefaultRuleExecutor) OnPublisherRemoved(id string, topicName string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicFragments, err := e.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	fragMappings := topicFragments.FragMappingInfo()
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
	topicFragmentFrame := topic.NewTopicFragmentsFrame(fragMappings)
	if err = e.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
		return err
	}
	logger.Info("update fragments to inactive state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	topicSubscriptions, err := e.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()
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

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = e.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnSubscriberAdded : when a subscriber connected, create new subscription for it and assign all fragments of topic.
func (e DefaultRuleExecutor) OnSubscriberAdded(id string, topicName string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicFragments, err := e.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	var allFragmentIds []uint
	for fragmentId, info := range topicFragments.FragMappingInfo() {
		if info.State == topic.Active {
			allFragmentIds = append(allFragmentIds, fragmentId)
		}
	}

	// update subscription info
	topicSubscriptions, err := e.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()
	subscriptionMappings[id] = allFragmentIds // assign new subscription for added subscriber

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = e.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("add subscription of subscriber", zap.String("topic", topicName), zap.String("subscriber", id))
	return nil
}

// OnSubscriberRemoved : when a subscriber disconnected, just delete subscription of it.
func (e DefaultRuleExecutor) OnSubscriberRemoved(id string, topicName string) error {
	lock := e.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicSubscriptions, err := e.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()
	delete(subscriptionMappings, id) // remove a subscription of removed subscriber

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = e.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
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
