package zk

import (
	"bytes"
	"github.com/paust-team/shapleq/coordinating"
	"os"
	"sync"
	"testing"
	"time"
)

// TODO:: refactor with BDD test using ginkgo

var zkCoord *CoordClient
var testBasePath = "/coordinator-test"
var testLockPath = "/coordinator-test-lock"

func testMainWrapper(m *testing.M) int {
	coordi := NewZKCoordClient([]string{"127.0.0.1"}, 3000)

	if err := coordi.Connect(); err != nil {
		panic("cannot connect zookeeper")
	}

	zkCoord = coordi
	err := zkCoord.Create(testBasePath, []byte{}).Run()
	err = zkCoord.Create(testLockPath, []byte{}).Run()
	if err != nil {
		panic("cannot create paths")
	}

	defer zkCoord.Close()
	defer func() { // delete all paths
		paths, err := coordi.Children(testBasePath).WithLock(testLockPath).Run()
		if err != nil {
			panic(err)
		}
		var deletePaths []string
		for _, path := range paths {
			deletePaths = append(deletePaths, testBasePath+"/"+path)
		}
		deletePaths = append(deletePaths, testBasePath, testLockPath)
		coordi.Delete(deletePaths).IgnoreError().Run()
	}()

	return m.Run()
}

func TestMain(m *testing.M) {
	os.Exit(testMainWrapper(m))
}

func TestCoordinator_Exists(t *testing.T) {
	testPath := testBasePath + "/test-exists"
	testData := []byte{'a'}

	if err := zkCoord.Create(testPath, testData).Run(); err != nil {
		t.Fatal(err)
	}

	exists, err := zkCoord.Exists(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("node exists but return false")
	}
}

func TestCoordinator_ExistsWatch_OnCreate(t *testing.T) {
	testPath := testBasePath + "/test-exists-watch-create"
	testData := []byte{'a'}
	wg := sync.WaitGroup{}
	wg.Add(1)
	exists, err := zkCoord.Exists(testPath).OnEvent(func(event coordinating.WatchEvent) coordinating.Recursive {
		defer wg.Done()
		if event.Type != coordinating.EventNodeCreated {
			t.Error("received wrong event", event.Type)
		}
		return false
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("node not exists but return true")
	}

	if err = zkCoord.Create(testPath, testData).Run(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}

func TestCoordinator_ExistsWatch_OnDelete(t *testing.T) {
	testPath := testBasePath + "/test-exists-watch-delete"
	testData := []byte{'a'}

	if err := zkCoord.Create(testPath, testData).Run(); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	exists, err := zkCoord.Exists(testPath).OnEvent(func(event coordinating.WatchEvent) coordinating.Recursive {
		defer wg.Done()
		if event.Type != coordinating.EventNodeDeleted {
			t.Error("received wrong event", event.Type)
		}
		return false
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("node exists but return false")
	}

	if err = zkCoord.Delete([]string{testPath}).Run(); err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func TestCoordinator_Get(t *testing.T) {
	testPath := testBasePath + "/test-get"
	testData := []byte{'a'}

	if err := zkCoord.Create(testPath, testData).Run(); err != nil {
		t.Fatal(err)
	}

	actualData, err := zkCoord.Get(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(testData, actualData) != 0 {
		t.Errorf("data not matched! expected(%s), received(%s)", testData, actualData)
	}
}

func TestCoordinator_Set(t *testing.T) {
	testPath := testBasePath + "/test-set"
	testData := []byte{'a'}

	if err := zkCoord.Create(testPath, []byte{}).Run(); err != nil {
		t.Fatal(err)
	}

	if err := zkCoord.Set(testPath, testData).WithLock(testLockPath).Run(); err != nil {
		t.Error(err)
	}

	actualData, err := zkCoord.Get(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(testData, actualData) != 0 {
		t.Errorf("data not matched! expected(%s), received(%s)", testData, actualData)
	}
}

func TestCoordinator_SetWatch(t *testing.T) {
	testPath := testBasePath + "/test-set-watch"
	testData := []byte{'a'}

	if err := zkCoord.Create(testPath, []byte{}).Run(); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	if _, err := zkCoord.Get(testPath).OnEvent(func(event coordinating.WatchEvent) coordinating.Recursive {
		defer wg.Done()
		if event.Type != coordinating.EventNodeDataChanged {
			t.Error("received wrong event", event.Type)
		}
		if event.Err != nil {
			t.Error(event.Err)
		}
		return false
	}).Run(); err != nil {
		t.Fatal(err)
	}

	wg.Add(1)

	if err := zkCoord.Set(testPath, testData).WithLock(testLockPath).Run(); err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func TestCoordinator_Children(t *testing.T) {
	pathName := "test-children"
	testPath := testBasePath + "/" + pathName

	if err := zkCoord.Create(testPath, []byte{}).Run(); err != nil {
		t.Fatal(err)
	}

	paths, err := zkCoord.Children(testBasePath).Run()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, path := range paths {
		if path == pathName {
			found = true
		}
	}

	if !found {
		t.Error("not found child path", pathName)
	}
}

func TestCoordinator_ChildrenWatch(t *testing.T) {
	testPath := testBasePath + "/test-children-watch-created"

	wg := sync.WaitGroup{}
	totalEvent := 2
	receivedEventCount := 0
	wg.Add(totalEvent)
	if _, err := zkCoord.Children(testBasePath).OnEvent(func(event coordinating.WatchEvent) coordinating.Recursive {
		defer wg.Done()
		receivedEventCount++
		if event.Type != coordinating.EventNodeChildrenChanged {
			t.Error("received wrong event", event.Type)
		}
		if event.Err != nil {
			t.Error(event.Err)
		}
		if receivedEventCount == totalEvent {
			return false
		} else {
			return true
		}

	}).Run(); err != nil {
		t.Fatal(err)
	}

	// create test
	if err := zkCoord.Create(testPath, []byte{}).Run(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second) // sleep for re-registering a watch
	// delete test
	if err := zkCoord.Delete([]string{testPath}).Run(); err != nil {
		t.Error(err)
	}
	wg.Wait()
}

func TestCoordinator_OptimisticUpdate(t *testing.T) {
	testPath := testBasePath + "/test-optimistic-update"
	firstData := []byte{'a'}
	secondData := []byte{'b'}
	thirdData := []byte{'c'}

	if err := zkCoord.Create(testPath, firstData).Run(); err != nil {
		t.Fatal(err)
	}

	if err := zkCoord.OptimisticUpdate(testPath, func(value []byte) []byte {
		if bytes.Compare(value, firstData) != 0 {
			// Set second value to incur optimistic lock.
			// It is a case when other concurrent request overwrites the value.
			if err := zkCoord.Set(testPath, secondData).Run(); err != nil {
				t.Fatal(err)
			}
			return secondData
		} else if bytes.Compare(value, secondData) != 0 {
			return thirdData
		}
		return []byte{}
	}).Run(); err != nil {
		t.Fatal(err)
	}

	actualData, err := zkCoord.Get(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(thirdData, actualData) != 0 {
		t.Errorf("data not matched! expected(%s), received(%s)", thirdData, actualData)
	}
}
