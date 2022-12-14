package inmemory

import (
	"context"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/logger"
	"strings"
	"sync"
)

type ChildrenOperation struct {
	m    *sync.Map
	path string
}

func NewInMemChildrenOperation(m *sync.Map, path string) ChildrenOperation {
	return ChildrenOperation{m: m, path: path}
}

func (o ChildrenOperation) WithLock(string) coordinating.ChildrenOperation {
	logger.Warn("ChildrenOperation.WithLock is not implement in in-mem coordinator")
	return o
}

func (o ChildrenOperation) Watch(context.Context) (<-chan coordinating.WatchEvent, error) {
	logger.Warn("ChildrenOperation.Watch is not implement in in-mem coordinator")
	return nil, nil
}

func (o ChildrenOperation) Run() ([]string, error) {
	var children []string

	o.m.Range(func(k, v interface{}) bool {
		path := k.(string)
		parentPath := o.path + "/"
		if strings.Contains(path, parentPath) {
			if sp := strings.Split(strings.Split(path, parentPath)[1], "/"); len(sp) == 1 {
				children = append(children, sp[0])
			}
		}
		return true
	})
	return children, nil
}
