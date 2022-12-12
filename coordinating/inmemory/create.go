package inmemory

import (
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/qerror"
	"sync"
)

type CreateOperation struct {
	m           *sync.Map
	path        string
	value       []byte
	isEphemeral bool
}

func NewInMemCreateOperation(m *sync.Map, path string, value []byte) CreateOperation {
	return CreateOperation{m: m, path: path, value: value}
}

func (o CreateOperation) WithLock(string) coordinating.CreateOperation {
	logger.Warn("CreateOperation.WithLock is not implement in in-mem coordinator")
	return o
}

func (o CreateOperation) AsEphemeral() coordinating.CreateOperation {
	o.isEphemeral = true
	return o
}

func (o CreateOperation) AsSequential() coordinating.CreateOperation {
	logger.Warn("CreateOperation.AsSequential is not implement in in-mem coordinator")
	return o
}

func (o CreateOperation) OnEvent(func(event coordinating.WatchEvent) coordinating.Recursive) coordinating.CreateOperation {
	logger.Warn("CreateOperation.OnEvent is not implement in in-mem coordinator")
	return o
}

func (o CreateOperation) Run() error {
	_, ok := o.m.Load(o.path)
	if ok {
		return qerror.CoordTargetAlreadyExistsError{Target: o.path}
	}
	o.m.Store(o.path, o.value)
	if o.isEphemeral {
		epPaths, _ := o.m.Load(ephemeralPath)
		o.m.Store(ephemeralPath, append(epPaths.([]string), o.path))
	}

	return nil
}
