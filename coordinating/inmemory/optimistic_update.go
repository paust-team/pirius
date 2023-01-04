package inmemory

import (
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/qerror"
	"sync"
)

type OptimisticUpdateOperation struct {
	m      *sync.Map
	path   string
	update func(current []byte) []byte
}

func NewInMemOptimisticUpdateOperation(m *sync.Map, path string, update func([]byte) []byte) OptimisticUpdateOperation {
	return OptimisticUpdateOperation{m: m, path: path, update: update}
}

func (o OptimisticUpdateOperation) Run() error {
	logger.Warn("Optimistic update in in-mem impl is same as normal set operation with update fn")
	value, ok := o.m.Load(o.path)
	if !ok {
		return qerror.CoordNoNodeError{Path: o.path}
	}
	data := o.update(value.([]byte))
	o.m.Store(o.path, data)

	return nil
}
