package helper

import (
	"encoding/binary"
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
)

type FrameForFragment struct {
	data []byte
}

func NewFrameForFragment(data []byte) *FrameForFragment {
	return &FrameForFragment{data: data}
}

func NewFrameForFragmentFromValues(lastOffset uint64, numSubscribers uint64) *FrameForFragment {
	data := make([]byte, uint64Len*2)
	binary.BigEndian.PutUint64(data[0:], lastOffset)
	binary.BigEndian.PutUint64(data[uint64Len:uint64Len*2], numSubscribers)
	return &FrameForFragment{data: data}
}

func (f FrameForFragment) Data() []byte {
	return f.data
}

func (f FrameForFragment) Size() int {
	return len(f.data)
}

func (f FrameForFragment) LastOffset() uint64 {
	return binary.BigEndian.Uint64(f.Data()[:uint64Len])
}

func (f *FrameForFragment) SetLastOffset(offset uint64) {
	binary.BigEndian.PutUint64(f.data[0:uint64Len], offset)
}

func (f FrameForFragment) NumSubscribers() uint64 {
	return binary.BigEndian.Uint64(f.Data()[uint64Len : uint64Len*2])
}

func (f *FrameForFragment) SetNumSubscribers(num uint64) {
	binary.BigEndian.PutUint64(f.data[uint64Len:uint64Len*2], num)
}

type FragmentManagingHelper struct {
	client coordinator.Coordinator
	logger *logger.QLogger
}

func NewFragmentManagingHelper(client coordinator.Coordinator) *FragmentManagingHelper {
	return &FragmentManagingHelper{client: client}
}

func (f *FragmentManagingHelper) WithLogger(logger *logger.QLogger) *FragmentManagingHelper {
	f.logger = logger
	return f
}

func (f *FragmentManagingHelper) GetTopicFragmentFrame(topicName string, fragmentId uint32) (*FrameForFragment, error) {
	if result, err := f.client.
		Get(constants.GetTopicFragmentPath(topicName, fragmentId)).
		Run(); err == nil {
		return NewFrameForFragment(result), nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicFragmentNotExistsError{Topic: topicName, FragmentId: fragmentId}
	} else {
		return nil, err
	}
}

func (f *FragmentManagingHelper) AddTopicFragmentFrame(topicName string, fragmentId uint32, fragmentFrame *FrameForFragment) error {
	if err := f.client.
		Create(constants.GetTopicFragmentPath(topicName, fragmentId), fragmentFrame.Data()).
		WithLock(constants.GetTopicFragmentLockPath(topicName)).
		Run(); err != nil {
		return err
	}
	if err := f.client.
		Create(constants.GetTopicFragmentBrokerBasePath(topicName, fragmentId), []byte{}).
		Run(); err != nil {
		if _, ok := err.(pqerror.ZKTargetAlreadyExistsError); ok { // ignore if node already exists
			return nil
		}
		return err
	}

	return nil
}

func (f *FragmentManagingHelper) RemoveTopicFragmentFrame(topicName string, fragmentId uint32) error {

	if err := f.client.
		Delete([]string{constants.GetTopicFragmentBrokerBasePath(topicName, fragmentId)}).
		WithLock(constants.GetTopicFragmentBrokerLockPath(topicName, fragmentId)).
		Run(); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		if f.logger != nil {
			f.logger.Error(err)
		}
		return err
	}

	if err := f.client.
		Delete([]string{constants.GetTopicFragmentBrokerLockPath(topicName, fragmentId), constants.GetTopicFragmentPath(topicName, fragmentId)}).
		WithLock(constants.GetTopicFragmentLockPath(topicName)).
		Run(); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		if f.logger != nil {
			f.logger.Error(err)
		}
		return err
	}

	return nil
}

func (f *FragmentManagingHelper) AddNumSubscriber(topicName string, fragmentId uint32, delta int) (uint64, error) {
	var numSubscribers uint64
	if err := f.client.OptimisticUpdate(constants.GetTopicFragmentPath(topicName, fragmentId), func(value []byte) []byte {
		fragmentFrame := NewFrameForFragment(value)
		count := int(fragmentFrame.NumSubscribers()) + delta
		fragmentFrame.SetNumSubscribers(uint64(count))
		numSubscribers = uint64(count)
		return fragmentFrame.Data()
	}).Run(); err != nil {
		return 0, err
	}

	return numSubscribers, nil
}
