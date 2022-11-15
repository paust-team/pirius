package rule

import (
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
)

type DistributionRuleExecutor struct {
	bootstrapper *bootstrapping.BootstrapService
}

func NewDistributionRuleExecutor(bootstrapper *bootstrapping.BootstrapService) DistributionRuleExecutor {
	return DistributionRuleExecutor{bootstrapper: bootstrapper}
}

// OnPublisherAdded : when a publisher connects, it either sets the fragment active or adds a new one until it completes num_subscribers.
// then assign newly active fragments to each subscription one by one (round-robin)
func (d DistributionRuleExecutor) OnPublisherAdded(id string, topicName string, host string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	// get topic fragment mappings
	topicFragments, err := d.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	fragMappings := topicFragments.FragMappingInfo()

	pubsFragmentIds, err := getFragmentsToPublish(d.bootstrapper.CoordClientTopicWrapper, topicName, id, fragMappings)
	if err != nil {
		return err
	}

	// get subscriptions
	topicSubscriptions, err := d.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()
	numSubscribers := len(subscriptionMappings)
	numPublishFragments := len(pubsFragmentIds)

	// when only one subscriber exists or does not exist, just active one fragment.
	// if not, set the number of fragments to the number of subscribers.

	if numPublishFragments < numSubscribers {
		// assign new fragment
		tempFragMappings := make(topic.FragMappingInfo)
		for _, fragmentId := range pubsFragmentIds {
			tempFragMappings[fragmentId] = topic.FragInfo{}
		}
		for i := 0; i < numSubscribers-numPublishFragments; i++ {
			newFragmentId, err := getAssignableFragmentId(topicName, tempFragMappings)
			if err != nil {
				return err
			}
			tempFragMappings[newFragmentId] = topic.FragInfo{}
			pubsFragmentIds = append(pubsFragmentIds, newFragmentId)
		}
	} else if numPublishFragments > numSubscribers {
		if numSubscribers == 0 {
			pubsFragmentIds = pubsFragmentIds[0:1]
		} else {
			pubsFragmentIds = pubsFragmentIds[0:numSubscribers]
		}
	}
	// assert numPublishFragments == numSubscribers

	// set publishing fragments as active
	for _, fragmentId := range pubsFragmentIds {
		fragMappings[fragmentId] = topic.FragInfo{
			State:       topic.Active,
			PublisherId: id,
			Address:     host,
		}
	}

	topicFragmentFrame := topic.NewTopicFragmentsFrame(fragMappings)
	if err = d.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
		return err
	}
	logger.Info("update fragments to active state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	// round-robin fragment assignment per subscriber
	selectFragment := helper.RoundRobinSelection(pubsFragmentIds)
	for subscriberId, subsFragmentIds := range subscriptionMappings {
		subscriptionMappings[subscriberId] = append(subsFragmentIds, selectFragment())
	}

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = d.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnPublisherRemoved : when a publisher is disconnected, set fragment as inactive and remove fragments from subscriptions
func (d DistributionRuleExecutor) OnPublisherRemoved(id string, topicName string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicFragments, err := d.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	fragMappings := topicFragments.FragMappingInfo()
	pubsFragmentIds, err := findPublisherFragments(d.bootstrapper.CoordClientTopicWrapper, topicName, id)
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
	if err = d.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
		return err
	}
	logger.Info("update fragments to inactive state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	topicSubscriptions, err := d.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()

	// exclude removed fragments from subscriptions
	for subscriberId, subsFragmentIds := range subscriptionMappings {
		var newSubsFragmentIds []uint
		for _, fragmentId := range subsFragmentIds {
			if !helper.IsContains(fragmentId, pubsFragmentIds) { // exclude inactive pubs fragments
				newSubsFragmentIds = append(newSubsFragmentIds, fragmentId)
			}
		}
		subscriptionMappings[subscriberId] = newSubsFragmentIds
	}

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = d.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnSubscriberAdded : when a subscriber connected, create new subscription for it and assign new 1*num_publisher fragments.
func (d DistributionRuleExecutor) OnSubscriberAdded(id string, topicName string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicFragments, err := d.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	fragMappings := topicFragments.FragMappingInfo()
	publisherInfoMap, numActivePublishers := topic.ConvertToPublisherInfo(fragMappings)
	if numActivePublishers == 0 {
		return qerror.InvalidStateError{State: "active publisher does not exist"}
	}

	topicSubscriptions, err := d.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()

	// if other subscriber does not exist, assign all active fragments of each alive publishers.
	// else, create a new fragment or change an inactive fragment of each publisher
	var subsFragmentIds []uint
	if len(subscriptionMappings) == 0 {
		for _, info := range publisherInfoMap {
			if !info.Alive {
				continue
			}
			subsFragmentIds = append(subsFragmentIds, info.ActiveFragments...)
		}
	} else {
		for publisherId, info := range publisherInfoMap {
			if !info.Alive {
				continue
			}
			var newFragmentId uint
			if len(info.InActiveFragments) == 0 {
				newFragmentId, err = getAssignableFragmentId(topicName, fragMappings)
				if err != nil {
					return err
				}
			} else {
				newFragmentId = info.InActiveFragments[0]
			}
			// update fragment info from new fragment assignment or state transition of inactive to active
			fragMappings[newFragmentId] = topic.FragInfo{
				State:       topic.Active,
				PublisherId: publisherId,
				Address:     info.Address,
			}
			topicFragmentFrame := topic.NewTopicFragmentsFrame(fragMappings)
			if err = d.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
				return err
			}
			subsFragmentIds = append(subsFragmentIds, newFragmentId)
		}
	}

	// update subscription info
	subscriptionMappings[id] = subsFragmentIds // assign new subscription for added subscriber

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = d.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("add subscription of subscriber", zap.String("topic", topicName), zap.String("subscriber", id))
	return nil
}

// OnSubscriberRemoved : when a subscriber disconnected, delete subscription of it and set subscribing fragments as stale
func (d DistributionRuleExecutor) OnSubscriberRemoved(id string, topicName string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	topicSubscriptions, err := d.bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return err
	}

	// remove a subscription of removed subscriber
	subscriptionMappings := topicSubscriptions.SubscriptionInfo()
	subscribingFragmentIds := subscriptionMappings[id]
	delete(subscriptionMappings, id)

	subscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionMappings)
	if err = d.bootstrapper.UpdateTopicSubscriptions(topicName, subscriptionFrame); err != nil {
		return err
	}
	logger.Info("remove subscription of subscriber", zap.String("topic", topicName), zap.String("subscriber", id))

	// set subscribing fragments as stale
	topicFragments, err := d.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return err
	}
	fragMappings := topicFragments.FragMappingInfo()
	for _, fragmentId := range subscribingFragmentIds {
		fragMappings[fragmentId] = topic.FragInfo{
			State:       topic.Stale,
			PublisherId: fragMappings[fragmentId].PublisherId,
			Address:     "",
		}
	}
	topicFragmentFrame := topic.NewTopicFragmentsFrame(fragMappings)
	if err = d.bootstrapper.UpdateTopicFragments(topicName, topicFragmentFrame); err != nil {
		return err
	}
	logger.Info("update fragments to stale state", zap.String("topic", topicName), zap.Uints("fragments", subscribingFragmentIds))

	return nil
}
