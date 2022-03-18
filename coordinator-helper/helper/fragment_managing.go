package helper

import (
	"encoding/binary"
	"fmt"
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"sync"
	"sync/atomic"
	"time"
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

type fragmentOffset struct {
	id         uint32
	lastOffset uint64
	topicName  string
}

func (t *fragmentOffset) LastOffset() uint64 {
	return atomic.LoadUint64(&t.lastOffset)
}
func (t *fragmentOffset) SetLastOffset(offset uint64) {
	atomic.StoreUint64(&t.lastOffset, offset)
}
func (t *fragmentOffset) IncreaseLastOffset() uint64 {
	return atomic.AddUint64(&t.lastOffset, 1)
}

type FragmentManagingHelper struct {
	client            coordinator.Coordinator
	fragmentOffsetMap sync.Map
	offsetFlusher     chan fragmentOffset
	logger            *logger.QLogger
}

func NewFragmentManagingHelper(client coordinator.Coordinator) *FragmentManagingHelper {
	return &FragmentManagingHelper{client: client, fragmentOffsetMap: sync.Map{}}
}

func (f *FragmentManagingHelper) WithLogger(logger *logger.QLogger) *FragmentManagingHelper {
	f.logger = logger
	return f
}

func (f *FragmentManagingHelper) StartPeriodicFlushLastOffsets(interval uint) {
	go func() {
		f.offsetFlusher = make(chan fragmentOffset, 100)
		defer func() {
			close(f.offsetFlusher)
			f.offsetFlusher = nil
		}()

		offsetMap := make(map[string]map[uint32]uint64)
		flushInterval := time.Millisecond * time.Duration(interval)
		timer := time.NewTimer(flushInterval)
		defer timer.Stop()

		for {
			select {
			case fragment := <-f.offsetFlusher:
				if offsetMap[fragment.topicName] == nil {
					offsetMap[fragment.topicName] = make(map[uint32]uint64)
				}
				offsetMap[fragment.topicName][fragment.id] = fragment.LastOffset()
			case <-timer.C:
				if f.client.IsClosed() {
					return
				}
				for topicName, fragments := range offsetMap {
					f.client.Lock(constants.GetTopicFragmentLockPath(topicName), func() {
						for fragmentId, offset := range fragments {
							value, err := f.client.
								Get(constants.GetTopicFragmentPath(topicName, fragmentId)).
								Run()
							if err != nil {
								if f.logger != nil {
									f.logger.Error(err)
								}
								continue
							}
							fragmentFrame := NewFrameForFragment(value)
							fragmentFrame.SetLastOffset(offset)
							if err := f.client.
								Set(constants.GetTopicFragmentPath(topicName, fragmentId), fragmentFrame.Data()).
								Run(); err != nil {
								if f.logger != nil {
									f.logger.Error(err)
								}
							}
						}
					}).Run()
				}
				offsetMap = make(map[string]map[uint32]uint64)
			}
			timer.Reset(flushInterval)
		}
	}()
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

func (f *FragmentManagingHelper) AddTopicFragment(topicName string, fragmentId uint32, fragmentFrame *FrameForFragment) error {
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

func (f *FragmentManagingHelper) RemoveTopicFragment(topicName string, fragmentId uint32) error {

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

func (f *FragmentManagingHelper) IncreaseLastOffset(topicName string, fragmentId uint32) (uint64, error) {
	fragmentOffsetMapKey := fmt.Sprintf("%s-%d", topicName, fragmentId)
	value, loaded := f.fragmentOffsetMap.LoadOrStore(fragmentOffsetMapKey, &fragmentOffset{fragmentId, 0, topicName})
	fragment, ok := value.(*fragmentOffset)
	if !ok {
		fragment = &fragmentOffset{fragmentId, 0, topicName}
	}

	if !loaded { // if fragmentOffsetMap is not initialized, load last offset from zookeeper
		fragmentFrame, err := f.GetTopicFragmentFrame(topicName, fragmentId)
		if err != nil {
			return 0, err
		}
		fragment.SetLastOffset(fragmentFrame.LastOffset())
	}
	offset := fragment.IncreaseLastOffset()
	if f.offsetFlusher != nil {
		f.offsetFlusher <- *fragment
	}
	return offset, nil
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
