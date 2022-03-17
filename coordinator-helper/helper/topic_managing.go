package helper

import (
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"strconv"
)

type TopicManagingHelper struct {
	client coordinator.Coordinator
	logger *logger.QLogger
}

func NewTopicManagerHelper(client coordinator.Coordinator) *TopicManagingHelper {
	return &TopicManagingHelper{client: client}
}

func (t *TopicManagingHelper) WithLogger(logger *logger.QLogger) *TopicManagingHelper {
	t.logger = logger
	return t
}

func (t *TopicManagingHelper) AddTopic(topicName string, topicFrame *common.FrameForTopic) error {
	if err := t.client.
		Create(constants.GetTopicPath(topicName), topicFrame.Data()).
		WithLock(constants.TopicsLockPath).
		Run(); err != nil {
		if _, ok := err.(pqerror.ZKTargetAlreadyExistsError); ok { // ignore creating duplicated topic path
			return nil
		}
		return err
	}

	if err := t.client.
		Create(constants.GetTopicFragmentBasePath(topicName), topicFrame.Data()).
		WithLock(constants.GetTopicFragmentLockPath(topicName)).
		Run(); err != nil {
		if _, ok := err.(pqerror.ZKTargetAlreadyExistsError); ok { // ignore creating duplicated topic fragment path
			return nil
		}
		return err
	}

	return nil
}

func (t *TopicManagingHelper) AddNumPublishers(topicName string, delta int) (uint64, error) {
	var numPublishers uint64
	if err := t.client.OptimisticUpdate(constants.GetTopicPath(topicName), func(value []byte) []byte {
		topicFrame := common.NewFrameForTopic(value)
		count := int(topicFrame.NumPublishers()) + delta
		if count < 0 {
			count = 0
		}
		topicFrame.SetNumPublishers(uint64(count))
		numPublishers = uint64(count)
		return topicFrame.Data()
	}).Run(); err != nil {
		return 0, err
	}

	return numPublishers, nil
}

func (t *TopicManagingHelper) GetTopicFrame(topicName string) (*common.FrameForTopic, error) {
	if result, err := t.client.
		Get(constants.GetTopicPath(topicName)).
		Run(); err == nil {
		return common.NewFrameForTopic(result), nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicNotExistError{Topic: topicName}
	} else {
		return nil, err
	}
}

func (t *TopicManagingHelper) GetTopics() ([]string, error) {
	if topics, err := t.client.
		Children(constants.TopicsPath).
		Run(); err != nil {
		return nil, err
	} else if len(topics) > 0 {
		return topics, nil
	} else {
		return nil, nil
	}
}

func (t *TopicManagingHelper) RemoveTopic(topicName string) error {
	var fragmentPaths []string
	if fragments, err := t.GetTopicFragments(topicName); err == nil {
		if fragments != nil {
			for _, fragment := range fragments {
				fragmentId, err := strconv.ParseUint(fragment, 10, 32)
				if err != nil {
					continue
				}
				fragmentPaths = append(fragmentPaths, constants.GetTopicFragmentPath(topicName, uint32(fragmentId)))
			}
		}
	}
	fragmentPaths = append(fragmentPaths, constants.GetTopicFragmentBasePath(topicName), constants.GetTopicFragmentLockPath(topicName))

	var err error
	t.client.Lock(constants.TopicsLockPath, func() {
		t.client.
			Delete(fragmentPaths).
			IgnoreError().
			Run()

		if err = t.client.
			Delete([]string{constants.GetTopicPath(topicName)}).
			Run(); err != nil {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
			if t.logger != nil {
				t.logger.Error(err)
			}
		}
	}).Run()

	return err
}

func (t *TopicManagingHelper) GetTopicFragments(topicName string) ([]string, error) {
	if fragments, err := t.client.
		Children(constants.GetTopicFragmentBasePath(topicName)).
		Run(); err != nil {
		return nil, err
	} else if len(fragments) > 0 {
		return fragments, nil
	} else {
		return nil, nil
	}
}

func (t *TopicManagingHelper) RemoveTopicPaths() {
	if topics, err := t.GetTopics(); err == nil {
		if topics != nil {
			var deletePaths []string
			for _, topic := range topics {
				if fragments, err := t.GetTopicFragments(topic); err == nil {
					if fragments != nil {
						for _, fragment := range fragments {
							fragmentId, err := strconv.ParseUint(fragment, 10, 32)
							if err != nil {
								continue
							}
							deletePaths = append(deletePaths,
								constants.GetTopicFragmentBrokerLockPath(topic, uint32(fragmentId)),
								constants.GetTopicFragmentBrokerBasePath(topic, uint32(fragmentId)),
								constants.GetTopicFragmentPath(topic, uint32(fragmentId)))
						}
					}
				}
				deletePaths = append(deletePaths, constants.GetTopicFragmentBasePath(topic), constants.GetTopicFragmentLockPath(topic), constants.GetTopicPath(topic))
			}
			t.client.
				Delete(deletePaths).
				WithLock(constants.TopicsLockPath).
				IgnoreError().
				Run()
		}
	}
}
