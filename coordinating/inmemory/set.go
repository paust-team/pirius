package inmemory

import (
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/qerror"
	"sync"
)

type SetOperation struct {
	m     *sync.Map
	path  string
	value []byte
}

func NewInMemSetOperation(m *sync.Map, path string, value []byte) SetOperation {
	return SetOperation{m: m, path: path, value: value}
}

func (o SetOperation) WithLock(string) coordinating.SetOperation {
	logger.Warn("SetOperation.OnEvent is not implement in in-mem coordinator")
	return o
}

func (o SetOperation) Run() error {
	_, ok := o.m.Load(o.path)
	if !ok {
		return qerror.CoordNoNodeError{Path: o.path}
	}
	o.m.Store(o.path, o.value)
	return nil
}
