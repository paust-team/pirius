package inmemory

import (
	"context"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/qerror"
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

func (o GetOperation) Watch(context.Context) (<-chan coordinating.WatchEvent, error) {
	logger.Warn("GetOperation.Watch is not implement in in-mem coordinator")
	return nil, nil
}

func (o GetOperation) Run() ([]byte, error) {
	v, ok := o.m.Load(o.path)
	if !ok {
		return nil, qerror.CoordNoNodeError{Path: o.path}
	}
	return v.([]byte), nil
}
