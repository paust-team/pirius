package inmemory

import (
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/logger"
	"strings"
	"sync"
)

type DeleteOperation struct {
	m     *sync.Map
	paths []string
}

func NewInMemDeleteOperation(m *sync.Map, paths []string) DeleteOperation {
	return DeleteOperation{m: m, paths: paths}
}

func (o DeleteOperation) WithLock(string) coordinating.DeleteOperation {
	logger.Warn("DeleteOperation.WithLock is not implement in in-mem coordinator")
	return o
}

func (o DeleteOperation) IgnoreError() coordinating.DeleteOperation {
	logger.Warn("DeleteOperation.IgnoreError is not implement in in-mem coordinator")
	return o
}

func (o DeleteOperation) Run() error {
	for _, p := range o.paths {
		deletePaths := []string{p}

		o.m.Range(func(k, v interface{}) bool {
			path := k.(string)
			if strings.Contains(path, p+"/") {
				deletePaths = append(deletePaths, path)
			}
			return true
		})
		for _, path := range deletePaths {
			o.m.Delete(path)
		}
	}

	return nil
}
