package topic

import (
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/qerror"
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
	var err error
	t.coordClient.Lock(path.TopicLockPath(topicName), func() {
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
		if err = t.coordClient.
			Delete([]string{path.TopicPath(topicName)}).
			Run(); err != nil {
			err = qerror.CoordRequestError{ErrStr: err.Error()}
		}
	}).Run()

	if err != nil {
		return err
	}
	// delete topic lock path
	return t.coordClient.
		Delete([]string{path.TopicLockPath(topicName)}).
		Run()
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
		WithLock(path.TopicLockPath(topicName)).
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
		WithLock(path.TopicLockPath(topicName)).
		Run(); err != nil {

		return err
	}
	return nil
}
