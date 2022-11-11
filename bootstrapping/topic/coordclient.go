package topic

import (
	"context"
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
)

type CoordClientTopicWrapper struct {
	coordClient coordinating.CoordClient
}

func NewCoordClientTopicWrapper(coordClient coordinating.CoordClient) CoordClientTopicWrapper {
	return CoordClientTopicWrapper{coordClient: coordClient}
}

func (t CoordClientTopicWrapper) CreateTopic(topicName string, topicFrame Frame) (err error) {
	defer func() {
		if err != nil {
			_ = t.DeleteTopic(topicName)
		}
	}()

	//create topic path
	if err = t.coordClient.Create(path.TopicPath(topicName), topicFrame.Data()).Run(); err != nil {
		if _, ok := err.(qerror.CoordTargetAlreadyExistsError); !ok { // skip when duplicated topic path
			return err
		}
		err = nil
	}

	// create topic fragments path
	emptyFragmentsFrame := NewTopicFragmentsFrame(make(FragMappingInfo))
	if err = t.coordClient.Create(path.TopicFragmentsPath(topicName), emptyFragmentsFrame.Data()).Run(); err != nil {
		if _, ok := err.(qerror.CoordTargetAlreadyExistsError); !ok { // skip when duplicated topic meta path
			return err
		}
		err = nil
	}

	// create topic subscriptions path
	emptySubscriptionsFrame := NewTopicSubscriptionsFrame(make(SubscriptionInfo))
	if err = t.coordClient.Create(path.TopicSubscriptionsPath(topicName), emptySubscriptionsFrame.Data()).Run(); err != nil {
		if _, ok := err.(qerror.CoordTargetAlreadyExistsError); !ok { // skip when duplicated topic meta path
			return err
		}
		err = nil
	}

	// create topic pubs path
	if err = t.coordClient.Create(path.TopicPubsPath(topicName), []byte{}).Run(); err != nil {
		if _, ok := err.(qerror.CoordTargetAlreadyExistsError); !ok { // skip when duplicated topic pubs path
			return err
		}
		err = nil
	}

	// create topic subs path
	if err = t.coordClient.Create(path.TopicSubsPath(topicName), []byte{}).Run(); err != nil {
		if _, ok := err.(qerror.CoordTargetAlreadyExistsError); !ok { // skip when duplicated topic subs path
			return err
		}
		err = nil
	}

	// create topic lock path
	if err := t.coordClient.Create(path.TopicLockPath(topicName), []byte{}).Run(); err != nil {
		if _, ok := err.(qerror.CoordTargetAlreadyExistsError); !ok { // skip when duplicated topic subs path
			return err
		}
		err = nil
	}
	return
}

func (t CoordClientTopicWrapper) GetTopic(topicName string) (Frame, error) {
	if result, err := t.coordClient.Get(path.TopicPath(topicName)).Run(); err == nil {
		return Frame{data: result}, nil
	} else if _, ok := err.(qerror.CoordNoNodeError); ok {
		return Frame{}, qerror.TopicNotExistError{Topic: topicName}
	} else {
		return Frame{}, err
	}
}

func (t CoordClientTopicWrapper) GetTopics() ([]string, error) {
	if topics, err := t.coordClient.Children(path.TopicsPath).Run(); err != nil {
		return nil, err
	} else if len(topics) > 0 {
		return topics, nil
	} else {
		return nil, nil
	}
}

func (t CoordClientTopicWrapper) DeleteTopic(topicName string) error {
	lock := t.coordClient.Lock(path.TopicLockPath(topicName))
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	topicSubPaths := []string{
		path.TopicFragmentsPath(topicName),
		path.TopicSubscriptionsPath(topicName),
		path.TopicPubsPath(topicName),
		path.TopicSubsPath(topicName),
	}
	// delete topic sub paths
	t.coordClient.
		Delete(topicSubPaths).
		IgnoreError().
		Run()

	// delete topic path
	if err := t.coordClient.
		Delete([]string{path.TopicPath(topicName)}).
		Run(); err != nil {
		return qerror.CoordRequestError{ErrStr: err.Error()}
	}
	lock.Unlock()
	// delete topic lock path
	return t.coordClient.
		Delete([]string{path.TopicLockPath(topicName)}).
		Run()
}

func (t CoordClientTopicWrapper) NewTopicLock(topicName string) coordinating.LockOperation {
	return t.coordClient.Lock(path.TopicLockPath(topicName))
}

func (t CoordClientTopicWrapper) GetTopicFragments(topicName string) (FragmentsFrame, error) {
	result, err := t.coordClient.Get(path.TopicFragmentsPath(topicName)).Run()
	if err == nil {
		return FragmentsFrame{data: result}, nil
	} else if _, ok := err.(qerror.CoordNoNodeError); ok {
		return FragmentsFrame{}, qerror.TopicNotExistError{Topic: topicName}
	} else {
		return FragmentsFrame{}, err
	}
}

func (t CoordClientTopicWrapper) UpdateTopicFragments(topicName string, fragmentsFrame FragmentsFrame) error {
	if err := t.coordClient.
		Set(path.TopicFragmentsPath(topicName), fragmentsFrame.Data()).
		Run(); err != nil {

		return err
	}
	return nil
}

func (t CoordClientTopicWrapper) GetTopicSubscriptions(topicName string) (SubscriptionsFrame, error) {
	result, err := t.coordClient.Get(path.TopicSubscriptionsPath(topicName)).Run()
	if err == nil {
		return SubscriptionsFrame{data: result}, nil
	} else if _, ok := err.(qerror.CoordNoNodeError); ok {
		return SubscriptionsFrame{}, qerror.TopicNotExistError{Topic: topicName}
	} else {
		return SubscriptionsFrame{}, err
	}
}

func (t CoordClientTopicWrapper) UpdateTopicSubscriptions(topicName string, subscriptionsFrame SubscriptionsFrame) error {
	if err := t.coordClient.
		Set(path.TopicSubscriptionsPath(topicName), subscriptionsFrame.Data()).
		Run(); err != nil {

		return err
	}
	return nil
}

func (t CoordClientTopicWrapper) AddPublisher(topicName string, id string, host string) error {
	return t.coordClient.
		Create(path.TopicPublisherPath(topicName, id), []byte(host)).
		AsEphemeral().
		Run()
}

func (t CoordClientTopicWrapper) GetPublisher(topicName string, id string) (string, error) {
	data, err := t.coordClient.Get(path.TopicPublisherPath(topicName, id)).Run()
	if err != nil {
		return "", err
	}
	return string(data[:]), nil
}

func (t CoordClientTopicWrapper) GetPublishers(topicName string) ([]string, error) {
	if pubs, err := t.coordClient.Children(path.TopicPubsPath(topicName)).Run(); err != nil {
		return nil, err
	} else if len(pubs) > 0 {
		return pubs, nil
	} else {
		return nil, nil
	}
}

func (t CoordClientTopicWrapper) AddSubscriber(topicName string, id string) error {
	return t.coordClient.
		Create(path.TopicSubscriberPath(topicName, id), []byte{}).
		AsEphemeral().
		Run()
}
func (t CoordClientTopicWrapper) GetSubscriber(topicName string, id string) (string, error) {
	data, err := t.coordClient.Get(path.TopicSubscriberPath(topicName, id)).Run()
	if err != nil {
		return "", err
	}
	return string(data[:]), nil
}

func (t CoordClientTopicWrapper) GetSubscribers(topicName string) ([]string, error) {
	if subs, err := t.coordClient.Children(path.TopicSubsPath(topicName)).Run(); err != nil {
		return nil, err
	} else if len(subs) > 0 {
		return subs, nil
	} else {
		return nil, nil
	}
}

// WatchPubsPathChanged : register a watcher on children changed and retrieve updated publishers
func (t CoordClientTopicWrapper) WatchPubsPathChanged(ctx context.Context, topicName string) (chan []string, error) {
	ch, err := t.coordClient.Children(path.TopicPubsPath(topicName)).Watch(ctx)
	if err != nil {
		return nil, err
	}

	pubsCh := make(chan []string)
	go func() {
		defer close(pubsCh)
		for event := range ch {
			if event.Type == coordinating.EventNodeChildrenChanged {
				publishers, err := t.GetPublishers(topicName)
				if err != nil {
					logger.Error("error occurred on receiving watch event", zap.Error(err))
				}
				select {
				case <-ctx.Done():
					logger.Debug("stop watching pubs path: parent ctx done")
					return
				case pubsCh <- publishers:
					logger.Debug("sent new pubs path to channel")
				}
			}
		}
	}()
	return pubsCh, nil
}

// WatchSubsPathChanged : register a watcher on children changed and retrieve updated subscribers
func (t CoordClientTopicWrapper) WatchSubsPathChanged(ctx context.Context, topicName string) (chan []string, error) {
	ch, err := t.coordClient.Children(path.TopicSubsPath(topicName)).Watch(ctx)
	if err != nil {
		return nil, err
	}

	subsCh := make(chan []string)
	go func() {
		defer close(subsCh)
		for event := range ch {
			if event.Type == coordinating.EventNodeChildrenChanged {
				subscribers, err := t.GetSubscribers(topicName)
				if err != nil {
					logger.Error("error occurred on receiving watch event", zap.Error(err))
				}
				select {
				case <-ctx.Done():
					logger.Debug("stop watching subs path: parent ctx done")
					return
				case subsCh <- subscribers:
					logger.Debug("sent new subs path to channel")
				}
			}
		}
	}()
	return subsCh, nil
}

// WatchFragmentInfoChanged : register a watcher on data changed and retrieve updated fragment info
func (t CoordClientTopicWrapper) WatchFragmentInfoChanged(ctx context.Context, topicName string) (chan FragMappingInfo, error) {
	ch, err := t.coordClient.Get(path.TopicFragmentsPath(topicName)).Watch(ctx)
	if err != nil {
		return nil, err
	}

	fragmentCh := make(chan FragMappingInfo)
	go func() {
		defer close(fragmentCh)
		for event := range ch {
			if event.Type == coordinating.EventNodeDataChanged {
				fragmentsFrame, err := t.GetTopicFragments(topicName)
				if err != nil {
					logger.Error("error occurred on receiving watch event", zap.Error(err))
				}
				select {
				case <-ctx.Done():
					logger.Debug("stop watching fragment info: parent ctx done")
					return
				case fragmentCh <- fragmentsFrame.FragMappingInfo():
					logger.Debug("sent new fragment info to channel")
				}
			}
		}
		logger.Debug("stop watching fragment info: parent channel closed")
	}()
	return fragmentCh, nil
}

// WatchSubscriptionChanged : register a watcher on data changed and retrieve updated subscription info
func (t CoordClientTopicWrapper) WatchSubscriptionChanged(ctx context.Context, topicName string) (chan SubscriptionInfo, error) {
	ch, err := t.coordClient.Get(path.TopicSubscriptionsPath(topicName)).Watch(ctx)
	if err != nil {
		return nil, err
	}

	subscriptionCh := make(chan SubscriptionInfo)
	go func() {
		defer close(subscriptionCh)
		for event := range ch {
			if event.Type == coordinating.EventNodeDataChanged {
				subscriptionFrame, err := t.GetTopicSubscriptions(topicName)
				if err != nil {
					logger.Error("error occurred on receiving watch event", zap.Error(err))
				}
				select {
				case <-ctx.Done():
					logger.Debug("stop watching subscription info: parent ctx done")
					return
				case subscriptionCh <- subscriptionFrame.SubscriptionInfo():
					logger.Debug("sent new subscription info to channel")
				}
			}
		}
		logger.Debug("stop watching subscription info: parent channel closed")
	}()
	return subscriptionCh, nil
}
