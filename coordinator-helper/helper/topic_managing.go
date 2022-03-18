package helper

import (
	"encoding/binary"
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"strconv"
	"unsafe"
)

var uint32Len = int(unsafe.Sizeof(uint32(0)))
var uint64Len = int(unsafe.Sizeof(uint64(0)))

type FrameForTopic struct {
	data []byte
}

func NewFrameForTopic(data []byte) *FrameForTopic {
	return &FrameForTopic{data: data}
}

func NewFrameForTopicFromValues(description string, numFragments uint32, replicationFactor uint32, numPublishers uint64) *FrameForTopic {

	data := make([]byte, uint64Len+uint32Len*2)
	binary.BigEndian.PutUint64(data[0:], numPublishers)
	binary.BigEndian.PutUint32(data[uint64Len:], numFragments)
	binary.BigEndian.PutUint32(data[uint64Len+uint32Len:], replicationFactor)
	data = append(data, description...)

	return &FrameForTopic{data: data}
}

func (t FrameForTopic) Data() []byte {
	return t.data
}

func (t FrameForTopic) Size() int {
	return len(t.data)
}

func (t FrameForTopic) NumPublishers() uint64 {
	return binary.BigEndian.Uint64(t.Data()[:uint64Len])
}

func (t *FrameForTopic) SetNumPublishers(num uint64) {
	binary.BigEndian.PutUint64(t.data[0:], num)
}

func (t FrameForTopic) NumFragments() uint32 {
	return binary.BigEndian.Uint32(t.Data()[uint64Len : uint64Len+uint32Len])
}

func (t FrameForTopic) ReplicationFactor() uint32 {
	return binary.BigEndian.Uint32(t.Data()[uint64Len+uint32Len : uint64Len+uint32Len*2])
}

func (t FrameForTopic) Description() string {
	return string(t.Data()[uint64Len+uint32Len*2:])
}

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

func (t *TopicManagingHelper) AddTopic(topicName string, topicFrame *FrameForTopic) error {
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
		topicFrame := NewFrameForTopic(value)
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

func (t *TopicManagingHelper) GetTopicFrame(topicName string) (*FrameForTopic, error) {
	if result, err := t.client.
		Get(constants.GetTopicPath(topicName)).
		Run(); err == nil {
		return NewFrameForTopic(result), nil
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
