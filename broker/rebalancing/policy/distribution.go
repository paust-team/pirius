package policy

import (
	"github.com/paust-team/pirius/bootstrapping"
	"github.com/paust-team/pirius/bootstrapping/topic"
	"github.com/paust-team/pirius/helper"
	"github.com/paust-team/pirius/logger"
	"go.uber.org/zap"
	"sync"
)

type DistributionPolicyExecutor struct {
	Executor
	flusher
}

func NewDistributionPolicyExecutor(bootstrapper *bootstrapping.BootstrapService) *DistributionPolicyExecutor {
	return &DistributionPolicyExecutor{
		flusher: flusher{
			bootstrapper: bootstrapper,
			mu:           sync.Mutex{},
		},
	}
}

// OnPublisherAdded : when a publisher connects, it either sets the fragment active or adds a new one until it completes num_subscribers.
// then assign newly active fragments to each subscription one by one (round-robin)
func (d *DistributionPolicyExecutor) OnPublisherAdded(id string, topicName string, host string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	// get topic fragment mappings
	fragMappings, err := d.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}
	pubsFragmentIds, err := getFragmentsToPublish(d.bootstrapper.CoordClientTopicWrapper, topicName, id, fragMappings)
	if err != nil {
		return err
	}

	subscribers, err := d.bootstrapper.GetSubscribers(topicName)
	if err != nil {
		return err
	}
	numSubscribers := len(subscribers)
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

	d.UpdateTopicFragments(topicName, fragMappings)
	logger.Info("update fragments to active state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	// round-robin fragment assignment per subscriber
	subscriptionMappings, err := d.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}
	selectFragment := helper.RoundRobinSelection(pubsFragmentIds)
	for _, subscriberId := range subscribers {
		subsFragmentIds := subscriptionMappings[subscriberId]
		subscriptionMappings[subscriberId] = append(subsFragmentIds, selectFragment())
	}

	d.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnPublisherRemoved : when a publisher is disconnected, set fragment as inactive and remove fragments from subscriptions
func (d *DistributionPolicyExecutor) OnPublisherRemoved(id string, topicName string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	fragMappings, err := d.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}
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

	d.UpdateTopicFragments(topicName, fragMappings)
	logger.Info("update fragments to inactive state", zap.String("topic", topicName), zap.Uints("fragments", pubsFragmentIds))

	// update subscription info
	subscriptionMappings, err := d.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}
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

	d.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("update subscription info", zap.String("topic", topicName))

	return nil
}

// OnSubscriberAdded : when a subscriber connected, create new subscription for it and assign new 1*num_publisher fragments.
func (d *DistributionPolicyExecutor) OnSubscriberAdded(id string, topicName string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	fragMappings, err := d.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}

	var subsFragmentIds []uint
	subscriptionMappings, err := d.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}

	if publisherInfoMap, numActivePublishers := topic.ConvertToPublisherInfo(fragMappings); numActivePublishers != 0 {

		// if other subscriber does not exist, assign all active fragments of each alive publishers.
		// else, create a new fragment or change an inactive fragment of each publisher
		if len(subscriptionMappings[id]) == numActivePublishers {
			logger.Info("skip assign subscription of subscriber",
				zap.String("topic", topicName),
				zap.String("subscriber", id),
				zap.Int("num-active-publishers", numActivePublishers),
				zap.Uints("assigned-fragments", subscriptionMappings[id]))
			return nil
		} else if len(subscriptionMappings) == 0 {
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
				subsFragmentIds = append(subsFragmentIds, newFragmentId)
			}
			d.UpdateTopicFragments(topicName, fragMappings)
		}
	}
	// update subscription info
	subscriptionMappings[id] = subsFragmentIds // assign new subscription for added subscriber
	d.UpdateSubscriptionMappings(topicName, subscriptionMappings)

	logger.Info("add subscription of subscriber",
		zap.String("topic", topicName), zap.String("subscriber", id), zap.Uints("fragments", subsFragmentIds))
	return nil
}

// OnSubscriberRemoved : when a subscriber disconnected, delete subscription of it and set subscribing fragments as stale
func (d *DistributionPolicyExecutor) OnSubscriberRemoved(id string, topicName string) error {
	lock := d.bootstrapper.NewTopicLock(topicName)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	subscriptionMappings, err := d.GetSubscriptionMappings(topicName)
	if err != nil {
		return err
	}
	// remove a subscription of removed subscriber

	subscribingFragmentIds := subscriptionMappings[id]
	delete(subscriptionMappings, id)

	d.UpdateSubscriptionMappings(topicName, subscriptionMappings)
	logger.Info("remove subscription of subscriber", zap.String("topic", topicName), zap.String("subscriber", id))

	// set subscribing fragments as stale when number of active fragment is greater than 1
	fragMappings, err := d.GetTopicFragmentMappings(topicName)
	if err != nil {
		return err
	}
	var activeFragmentIds []uint
	for fragmentId, mapping := range fragMappings {
		if mapping.State == topic.Active {
			activeFragmentIds = append(activeFragmentIds, fragmentId)
		}
	}

	if helper.HasAllElements(activeFragmentIds, subscribingFragmentIds) {
		logger.Info("skip update fragments: at least one fragment of each publisher should be active state",
			zap.String("topic", topicName),
			zap.Uints("total-active-fragments", activeFragmentIds),
			zap.Uints("subscribing-fragments", subscribingFragmentIds))
		return nil
	}
	for _, fragmentId := range subscribingFragmentIds {
		fragMappings[fragmentId] = topic.FragInfo{
			State:       topic.Stale,
			PublisherId: fragMappings[fragmentId].PublisherId,
			Address:     "",
		}
	}
	d.UpdateTopicFragments(topicName, fragMappings)

	logger.Info("update fragments to stale state", zap.String("topic", topicName), zap.Uints("fragments", subscribingFragmentIds))

	return nil
}
