package zookeeper

import (
	"bytes"
	"fmt"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper/constants"
	"github.com/samuel/go-zookeeper/zk"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type bootstrappingHelper struct {
	client *zkClientWrapper
	logger *logger.QLogger
}

// broker related methods

func (b bootstrappingHelper) GetBrokers() ([]string, error) {
	if brokersBytes, err := b.client.Get(constants.BrokersPath); err == nil {
		if len(brokersBytes) == 0 {
			b.client.Logger().Info("no broker exists")
			return nil, nil
		}

		brokers := strings.Split(bytes.NewBuffer(brokersBytes).String(), ",")
		return brokers, nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.BrokerNotExistsError{}
	} else {
		return nil, err
	}
}

func (b bootstrappingHelper) AddBroker(hostName string) error {
	brokers, err := b.GetBrokers()
	if err != nil {
		return err
	}

	for _, broker := range brokers {
		if broker == hostName {
			b.client.Logger().Info("broker already exists")
			return nil
		}
	}

	brokers = append(brokers, hostName)
	if err = b.client.Set(constants.BrokersLockPath, constants.BrokersPath, []byte(strings.Join(brokers, ","))); err != nil {
		return err
	}

	return nil
}

func (b bootstrappingHelper) RemoveBroker(hostName string) error {
	brokers, err := b.GetBrokers()
	if err != nil {
		return err
	}

	found := false
	for i, broker := range brokers {
		if broker == hostName {
			brokers = append(brokers[:i], brokers[i+1:]...)
			found = true
			break
		}
	}

	if found == false {
		return pqerror.ZKNothingToRemoveError{}
	}

	if err = b.client.Set(constants.BrokersLockPath, constants.BrokersPath, []byte(strings.Join(brokers, ","))); err != nil {
		return err
	}

	return nil
}

func (b bootstrappingHelper) GetBrokersOfTopic(topicName string, fragmentId uint32) ([]string, error) {
	if brokersBytes, err := b.client.Get(GetTopicFragmentBrokerBasePath(topicName, fragmentId)); err == nil {
		if len(brokersBytes) == 0 {
			b.logger.Info("no broker exists")
			return nil, nil
		}

		topicFragmentBrokers := strings.Split(bytes.NewBuffer(brokersBytes).String(), ",")
		return topicFragmentBrokers, nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicFragmentBrokerNotExistsError{Topic: topicName, FragmentId: fragmentId}
	} else {
		return nil, err
	}
}

func (b bootstrappingHelper) AddBrokerForTopic(topicName string, fragmentId uint32, hostName string) error {
	topicBrokers, err := b.GetBrokersOfTopic(topicName, fragmentId)
	if err != nil {
		return err
	}

	for _, topicBroker := range topicBrokers {
		if topicBroker == hostName {
			b.logger.Info(pqerror.ZKTargetAlreadyExistsError{Target: hostName})
			return nil
		}
	}

	topicBrokers = append(topicBrokers, hostName)
	if err = b.client.Set(constants.TopicFragmentsLockPath, GetTopicFragmentBrokerBasePath(topicName, fragmentId), []byte(strings.Join(topicBrokers, ","))); err != nil {
		return err
	}

	return nil
}

func (b bootstrappingHelper) RemoveBrokerOfTopic(topicName string, fragmentId uint32, hostName string) error {
	topicFragmentBrokers, err := b.GetBrokersOfTopic(topicName, fragmentId)
	if err != nil {
		return err
	}

	found := false
	for i, broker := range topicFragmentBrokers {
		if broker == hostName {
			topicFragmentBrokers = append(topicFragmentBrokers[:i], topicFragmentBrokers[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		err = pqerror.ZKNothingToRemoveError{}
		b.logger.Error(err)
		return err
	}

	if err = b.client.Set(constants.TopicFragmentsLockPath, GetTopicFragmentBrokerBasePath(topicName, fragmentId), []byte(strings.Join(topicFragmentBrokers, ","))); err != nil {
		return err
	}

	return nil
}

// topic related methods

type topicManagingHelper struct {
	client *zkClientWrapper
	logger *logger.QLogger
}

func (t *topicManagingHelper) AddTopic(topicName string, topicData *common.FrameForTopic) error {
	if err := t.client.Create(constants.TopicsLockPath, GetTopicPath(topicName), topicData.Data()); err != nil {
		if err == zk.ErrNodeExists { // ignore creating duplicated topic path
			return nil
		}
		return err
	}

	if err := t.client.CreatePathIfNotExists(GetTopicFragmentBasePath(topicName)); err != nil {
		return err
	}

	return nil
}

func (t *topicManagingHelper) AddNumPublishers(topicName string, delta int) (uint64, error) {
	var numPublishers uint64
	if err := t.client.OptimisticSet(GetTopicPath(topicName), func(value []byte) []byte {
		topicData := common.NewTopicData(value)
		count := int(topicData.NumPublishers()) + delta
		if count < 0 {
			count = 0
		}
		topicData.SetNumPublishers(uint64(count))
		numPublishers = uint64(count)
		return topicData.Data()
	}); err != nil {
		return 0, err
	}

	return numPublishers, nil
}

func (t *topicManagingHelper) GetTopicData(topicName string) (*common.FrameForTopic, error) {
	if result, err := t.client.Get(GetTopicPath(topicName)); err == nil {
		return common.NewTopicData(result), nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicNotExistError{Topic: topicName}
	} else {
		return nil, err
	}
}

func (t *topicManagingHelper) GetTopics() ([]string, error) {
	if topics, err := t.client.Children(constants.TopicsLockPath, constants.TopicsPath); err != nil {
		return nil, err
	} else if len(topics) > 0 {
		return topics, nil
	} else {
		return nil, nil
	}
}

func (t *topicManagingHelper) RemoveTopic(topicName string) error {
	var fragmentPaths []string
	if fragments, err := t.GetTopicFragments(topicName); err == nil {
		if fragments != nil {
			for _, fragment := range fragments {
				fragmentId, err := strconv.ParseUint(fragment, 10, 32)
				if err != nil {
					continue
				}
				fragmentPaths = append(fragmentPaths, GetTopicFragmentPath(topicName, uint32(fragmentId)))
			}
		}
	}
	fragmentPaths = append(fragmentPaths, GetTopicFragmentBasePath(topicName))

	t.client.DeleteAll(constants.TopicsLockPath, fragmentPaths)

	if err := t.client.Delete(constants.TopicsLockPath, GetTopicPath(topicName)); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		t.logger.Error(err)
		return err
	}

	return nil
}

func (t *topicManagingHelper) GetTopicFragments(topicName string) ([]string, error) {
	if fragments, err := t.client.Children(constants.TopicsLockPath, GetTopicFragmentBasePath(topicName)); err != nil {
		return nil, err
	} else if len(fragments) > 0 {
		return fragments, nil
	} else {
		return nil, nil
	}
}

func (t *topicManagingHelper) RemoveTopicPaths() {
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
							deletePaths = append(deletePaths, GetTopicFragmentBrokerBasePath(topic, uint32(fragmentId)))
							deletePaths = append(deletePaths, GetTopicFragmentPath(topic, uint32(fragmentId)))
						}
					}
				}
				deletePaths = append(deletePaths, GetTopicFragmentBasePath(topic))
				deletePaths = append(deletePaths, GetTopicPath(topic))
			}
			t.client.DeleteAll("", deletePaths)
		}
	}
}

// fragment related methods

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

type fragmentManagingHelper struct {
	client            *zkClientWrapper
	logger            *logger.QLogger
	fragmentOffsetMap sync.Map
	offsetFlusher     chan fragmentOffset
}

func (f *fragmentManagingHelper) startPeriodicFlushLastOffsets(interval uint) {
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
					for fragmentId, offset := range fragments {
						_ = f.client.OptimisticSet(GetTopicFragmentPath(topicName, fragmentId), func(value []byte) []byte {
							fragmentData := common.NewFragmentData(value)
							fragmentData.SetLastOffset(offset)
							return fragmentData.Data()
						})
					}
				}
				offsetMap = make(map[string]map[uint32]uint64)
			}
			timer.Reset(flushInterval)
		}
	}()
}

func (f *fragmentManagingHelper) GetTopicFragmentData(topicName string, fragmentId uint32) (*common.FrameForFragment, error) {
	if result, err := f.client.Get(GetTopicFragmentPath(topicName, fragmentId)); err == nil {
		return common.NewFragmentData(result), nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicFragmentNotExistsError{Topic: topicName, FragmentId: fragmentId}
	} else {
		return nil, err
	}
}

func (f *fragmentManagingHelper) AddTopicFragment(topicName string, fragmentId uint32, fragmentData *common.FrameForFragment) error {
	if err := f.client.Create(constants.TopicFragmentsLockPath, GetTopicFragmentPath(topicName, fragmentId), fragmentData.Data()); err != nil {
		if err == zk.ErrNodeExists {
			return &pqerror.ZKTargetAlreadyExistsError{Target: fmt.Sprintf("%s/%d", topicName, fragmentId)}
		}
		return err
	}
	if err := f.client.CreatePathIfNotExists(GetTopicFragmentBrokerBasePath(topicName, fragmentId)); err != nil {
		return err
	}

	return nil
}

func (f *fragmentManagingHelper) RemoveTopicFragment(topicName string, fragmentId uint32) error {

	if err := f.client.Delete(constants.TopicFragmentsLockPath, GetTopicFragmentBrokerBasePath(topicName, fragmentId)); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		f.logger.Error(err)
		return err
	}

	if err := f.client.Delete(constants.TopicFragmentsLockPath, GetTopicFragmentPath(topicName, fragmentId)); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		f.logger.Error(err)
		return err
	}

	return nil
}

func (f *fragmentManagingHelper) IncreaseLastOffset(topicName string, fragmentId uint32) (uint64, error) {
	fragmentOffsetMapKey := fmt.Sprintf("%s-%d", topicName, fragmentId)
	value, loaded := f.fragmentOffsetMap.LoadOrStore(fragmentOffsetMapKey, &fragmentOffset{fragmentId, 0, topicName})
	fragment, ok := value.(*fragmentOffset)
	if !ok {
		fragment = &fragmentOffset{fragmentId, 0, topicName}
	}

	if !loaded { // if fragmentOffsetMap is not initialized, load last offset from zk
		fragmentData, err := f.GetTopicFragmentData(topicName, fragmentId)
		if err != nil {
			return 0, err
		}
		fragment.SetLastOffset(fragmentData.LastOffset())
	}
	offset := fragment.IncreaseLastOffset()
	if f.offsetFlusher != nil {
		f.offsetFlusher <- *fragment
	}
	return offset, nil
}

func (f *fragmentManagingHelper) AddNumSubscriber(topicName string, fragmentId uint32, delta int) (uint64, error) {
	var numSubscribers uint64
	if err := f.client.OptimisticSet(GetTopicFragmentPath(topicName, fragmentId), func(value []byte) []byte {
		fragmentData := common.NewFragmentData(value)
		count := int(fragmentData.NumSubscribers()) + delta
		fragmentData.SetNumSubscribers(uint64(count))
		numSubscribers = uint64(count)
		return fragmentData.Data()
	}); err != nil {
		return 0, err
	}

	return numSubscribers, nil
}
