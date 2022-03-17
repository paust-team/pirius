package coordinator_helper

import (
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	"github.com/paust-team/shapleq/coordinator-helper/helper"
	zk_impl "github.com/paust-team/shapleq/coordinator/zk-impl"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
)

type CoordinatorWrapper struct {
	*helper.BootstrappingHelper
	*helper.TopicManagingHelper
	*helper.FragmentManagingHelper
	quorums       []string
	timeout       uint
	flushInterval uint
	logger        *logger.QLogger
	zkCoordinator *zk_impl.Coordinator
}

func NewCoordinatorWrapper(quorums []string, timeout uint, flushInterval uint, qLogger *logger.QLogger) *CoordinatorWrapper {
	return &CoordinatorWrapper{
		quorums:       quorums,
		timeout:       timeout,
		logger:        qLogger,
		flushInterval: flushInterval,
	}
}

func (q *CoordinatorWrapper) Connect() error {
	zkCoordinator, err := zk_impl.NewZKCoordinator(q.quorums, q.timeout, q.logger)
	if err != nil {
		err = pqerror.ZKConnectionError{ZKAddrs: q.quorums}
		return err
	}

	q.zkCoordinator = zkCoordinator
	q.BootstrappingHelper = helper.NewBootstrappingHelper(zkCoordinator)
	q.TopicManagingHelper = helper.NewTopicManagerHelper(zkCoordinator)
	q.FragmentManagingHelper = helper.NewFragmentManagingHelper(zkCoordinator)

	if q.flushInterval > 0 {
		q.FragmentManagingHelper.StartPeriodicFlushLastOffsets(q.flushInterval)
	}

	return nil
}

func (q CoordinatorWrapper) Close() {
	q.zkCoordinator.Close()
}

func (q CoordinatorWrapper) CreatePathsIfNotExist() error {
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

// for testing
func (q *CoordinatorWrapper) RemoveAllPath() {
	q.RemoveTopicPaths()
	deletePaths := []string{constants.TopicsPath, constants.BrokersPath, constants.BrokersLockPath,
		constants.TopicsLockPath, constants.BrokersLockPath, constants.ShapleQPath}

	q.zkCoordinator.Delete(deletePaths).IgnoreError().Run()
}
