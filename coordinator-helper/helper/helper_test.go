package helper

import (
	"fmt"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	zk_impl "github.com/paust-team/shapleq/coordinator/zk-impl"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"os"
	"testing"
)

type helperClient struct {
	*zk_impl.Coordinator
}

func (z helperClient) CreatePathsIfNotExist() error {
	paths := []string{constants.ShapleQPath, constants.BrokersPath, constants.TopicsPath, constants.BrokersLockPath, constants.TopicsLockPath}
	for _, path := range paths {
		if err := z.Create(path, []byte{}).Run(); err != nil {
			if _, ok := err.(pqerror.ZKTargetAlreadyExistsError); ok { // ignore if node already exists
				continue
			}
			return err
		}
	}
	return nil
}

// for testing
func (z *helperClient) RemoveAllPath() {
	topicHelper := NewTopicManagerHelper(z.Coordinator)
	topicHelper.RemoveTopicPaths()

	deletePaths := []string{constants.TopicsPath, constants.BrokersPath, constants.BrokersLockPath,
		constants.TopicsLockPath, constants.BrokersLockPath, constants.ShapleQPath}
	fmt.Println(z.Delete(deletePaths).Run())
}

var client *helperClient

func testMainWrapper(m *testing.M) int {
	zkClient, err := zk_impl.NewZKCoordinator([]string{"127.0.0.1"}, 3000, logger.NewQLogger("helper-test", logger.Info))

	if err != nil {
		panic("cannot connect zookeeper")
	}

	client = &helperClient{zkClient}
	err = client.CreatePathsIfNotExist()
	if err != nil {
		panic("cannot create paths")
	}
	defer client.Close()
	defer client.RemoveAllPath()

	return m.Run()
}

func TestMain(m *testing.M) {
	os.Exit(testMainWrapper(m))
}
