package inmemory

import (
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
func (o ExistsOperation) OnEvent(func(event coordinating.WatchEvent) coordinating.Recursive) coordinating.ExistsOperation {
	logger.Warn("ExistsOperation.OnEvent is not implement in in-mem coordinator")
	return o
}

func (o ExistsOperation) Run() (bool, error) {
	_, ok := o.m.Load(o.path)
	return ok, nil
}
