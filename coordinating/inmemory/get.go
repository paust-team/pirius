package inmemory

import (
	"github.com/paust-team/shapleq/agent/logger"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/qerror"
	"sync"
)

type GetOperation struct {
	m    *sync.Map
	path string
}

func NewInMemGetOperation(m *sync.Map, path string) GetOperation {
	return GetOperation{m: m, path: path}
}

func (o GetOperation) WithLock(string) coordinating.GetOperation {
	logger.Warn("GetOperation.WithLock is not implement in in-mem coordinator")
	return o
}

func (o GetOperation) OnEvent(fn func(coordinating.WatchEvent) coordinating.Recursive) coordinating.GetOperation {
	logger.Warn("GetOperation.OnEvent is not implement in in-mem coordinator")
	return o
}

func (o GetOperation) Run() ([]byte, error) {
	v, ok := o.m.Load(o.path)
	if !ok {
		return nil, qerror.CoordNoNodeError{Path: o.path}
	}
	return v.([]byte), nil
}
