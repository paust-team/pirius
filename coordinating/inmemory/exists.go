package inmemory

import (
	"context"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/logger"
	"sync"
)

type ExistsOperation struct {
	m    *sync.Map
	path string
}

func NewInMemExistsOperation(m *sync.Map, path string) ExistsOperation {
	return ExistsOperation{m: m, path: path}
}

func (o ExistsOperation) WithLock(string) coordinating.ExistsOperation {
	logger.Warn("ExistsOperation.WithLock is not implement in in-mem coordinator")
	return o
}

func (o ExistsOperation) Watch(context.Context) (<-chan coordinating.WatchEvent, error) {
	logger.Warn("ExistsOperation.Watch is not implement in in-mem coordinator")
	return nil, nil
}

func (o ExistsOperation) Run() (bool, error) {
	_, ok := o.m.Load(o.path)
	return ok, nil
}
