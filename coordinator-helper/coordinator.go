package coordinator_helper

import (
	"fmt"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	"github.com/paust-team/shapleq/coordinator-helper/helper"
	zk_impl "github.com/paust-team/shapleq/coordinator/zk-impl"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"sync"
	"sync/atomic"
	"time"
)

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

type CoordinatorWrapper struct {
	*helper.BootstrappingHelper
	*helper.TopicManagingHelper
	*helper.FragmentManagingHelper
	quorum            []string
	timeout           uint
	flushInterval     uint
	logger            *logger.QLogger
	zkCoordinator     *zk_impl.Coordinator
	fragmentOffsetMap sync.Map
	offsetFlusher     chan fragmentOffset
}

func NewCoordinatorWrapper(quorum []string, timeout uint, flushInterval uint, qLogger *logger.QLogger) *CoordinatorWrapper {
	return &CoordinatorWrapper{
		quorum:        quorum,
		timeout:       timeout,
		logger:        qLogger,
		flushInterval: flushInterval,
	}
}

func (q *CoordinatorWrapper) Connect() error {
	zkCoordinator, err := zk_impl.NewZKCoordinator(q.quorum, q.timeout, q.logger)
	if err != nil {
		err = pqerror.ZKConnectionError{ZKAddrs: q.quorum}
		return err
	}

	q.zkCoordinator = zkCoordinator
	q.BootstrappingHelper = helper.NewBootstrappingHelper(zkCoordinator)
	q.TopicManagingHelper = helper.NewTopicManagerHelper(zkCoordinator)
	q.FragmentManagingHelper = helper.NewFragmentManagingHelper(zkCoordinator)

	if q.flushInterval > 0 {
		q.startPeriodicFlushLastOffsets(q.flushInterval)
	}

	return nil
}

func (q *CoordinatorWrapper) Close() {
	q.zkCoordinator.Close()
}

func (q *CoordinatorWrapper) CreatePathsIfNotExist() error {
	paths := []string{constants.ShapleQPath, constants.BrokersPath, constants.TopicsPath, constants.BrokersLockPath, constants.TopicsLockPath}
	for _, path := range paths {
		if err := q.zkCoordinator.Create(path, []byte{}).Run(); err != nil {
			if _, ok := err.(pqerror.ZKTargetAlreadyExistsError); ok { // ignore if node already exists
				continue
			}
			return err
		}
	}
	return nil
}

func (q *CoordinatorWrapper) startPeriodicFlushLastOffsets(interval uint) {
	go func() {
		q.offsetFlusher = make(chan fragmentOffset, 100)
		defer func() {
			close(q.offsetFlusher)
			q.offsetFlusher = nil
		}()

		offsetMap := make(map[string]map[uint32]uint64)
		flushInterval := time.Millisecond * time.Duration(interval)
		timer := time.NewTimer(flushInterval)
		defer timer.Stop()

		for {
			select {
			case fragment := <-q.offsetFlusher:
				if offsetMap[fragment.topicName] == nil {
					offsetMap[fragment.topicName] = make(map[uint32]uint64)
				}
				offsetMap[fragment.topicName][fragment.id] = fragment.LastOffset()
			case <-timer.C:
				if q.zkCoordinator.IsClosed() {
					return
				}
				for topicName, fragments := range offsetMap {
					q.zkCoordinator.Lock(constants.GetTopicFragmentLockPath(topicName), func() {
						for fragmentId, offset := range fragments {
							fragmentFrame, err := q.GetTopicFragmentFrame(topicName, fragmentId)
							if err != nil {
								if q.logger != nil {
									q.logger.Error(err)
								}
								continue
							}
							fragmentFrame.SetLastOffset(offset)
							if err := q.zkCoordinator.
								Set(constants.GetTopicFragmentPath(topicName, fragmentId), fragmentFrame.Data()).
								Run(); err != nil {
								if q.logger != nil {
									q.logger.Error(err)
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

// for testing
func (q *CoordinatorWrapper) RemoveAllPath() {
	q.RemoveTopicPaths()
	deletePaths := []string{constants.TopicsPath, constants.BrokersPath, constants.BrokersLockPath,
		constants.TopicsLockPath, constants.BrokersLockPath, constants.ShapleQPath}

	q.zkCoordinator.Delete(deletePaths).IgnoreError().Run()
}

func (q *CoordinatorWrapper) IncreaseLastOffset(topicName string, fragmentId uint32) (uint64, error) {
	fragmentOffsetMapKey := fmt.Sprintf("%s-%d", topicName, fragmentId)
	value, loaded := q.fragmentOffsetMap.LoadOrStore(fragmentOffsetMapKey, &fragmentOffset{fragmentId, 0, topicName})
	fragment, ok := value.(*fragmentOffset)
	if !ok {
		fragment = &fragmentOffset{fragmentId, 0, topicName}
	}

	if !loaded { // if fragmentOffsetMap is not initialized, load last offset from zookeeper
		fragmentFrame, err := q.GetTopicFragmentFrame(topicName, fragmentId)
		if err != nil {
			return 0, err
		}
		fragment.SetLastOffset(fragmentFrame.LastOffset())
	}
	offset := fragment.IncreaseLastOffset()
	if q.offsetFlusher != nil {
		q.offsetFlusher <- *fragment
	}
	return offset, nil
}
