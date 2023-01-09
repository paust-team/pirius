package inmemory

import (
	"bytes"
	"os"
	"testing"
)

var inMemCoord *CoordClient
var testBasePath = "/coordinator-test"

// TODO:: refactor with BDD test using ginkgo

func testMainWrapper(m *testing.M) int {
	coordi := NewInMemCoordClient()

	inMemCoord = coordi
	err := inMemCoord.Create(testBasePath, []byte{}).Run()
	if err != nil {
		panic("cannot create paths")
	}

	defer inMemCoord.Close()
	defer func() { // delete all paths
		deletePaths := []string{testBasePath}
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

	if err := inMemCoord.Create(testPath, testData).Run(); err != nil {
		t.Fatal(err)
	}

	exists, err := inMemCoord.Exists(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("node exists but return false")
	}
}

func TestCoordinator_Get(t *testing.T) {
	testPath := testBasePath + "/test-get"
	testData := []byte{'a'}

	if err := inMemCoord.Create(testPath, testData).Run(); err != nil {
		t.Fatal(err)
	}

	actualData, err := inMemCoord.Get(testPath).Run()
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

	if err := inMemCoord.Create(testPath, []byte{}).Run(); err != nil {
		t.Fatal(err)
	}

	if err := inMemCoord.Set(testPath, testData).Run(); err != nil {
		t.Error(err)
	}

	actualData, err := inMemCoord.Get(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(testData, actualData) != 0 {
		t.Errorf("data not matched! expected(%s), received(%s)", testData, actualData)
	}
}

func TestCoordinator_Children(t *testing.T) {
	pathName := "test-children"
	testPath := testBasePath + "/" + pathName

	if err := inMemCoord.Create(testPath, []byte{}).Run(); err != nil {
		t.Fatal(err)
	}

	paths, err := inMemCoord.Children(testBasePath).Run()
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

func TestCoordinator_OptimisticUpdate(t *testing.T) {
	testPath := testBasePath + "/test-optimistic-update"
	firstData := []byte{'a'}
	secondData := []byte{'b'}
	thirdData := []byte{'c'}

	if err := inMemCoord.Create(testPath, firstData).Run(); err != nil {
		t.Fatal(err)
	}

	if err := inMemCoord.OptimisticUpdate(testPath, func(value []byte) []byte {
		if bytes.Compare(value, firstData) != 0 {
			// Set second value to incur optimistic lock.
			// It is a case when other concurrent request overwrites the value.
			if err := inMemCoord.Set(testPath, secondData).Run(); err != nil {
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

	actualData, err := inMemCoord.Get(testPath).Run()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(thirdData, actualData) != 0 {
		t.Errorf("data not matched! expected(%s), received(%s)", thirdData, actualData)
	}
}
