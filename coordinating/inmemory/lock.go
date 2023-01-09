package inmemory

import (
	"github.com/paust-team/pirius/logger"
)

type LockOperation struct {
}

func NewInMemLockOperation() LockOperation {
	return LockOperation{}
}

func (o LockOperation) Lock() error {
	logger.Warn("not fully implement in in-mem coordinator")
	return nil
}

func (o LockOperation) Unlock() error {
	logger.Warn("not fully implement in in-mem coordinator")
	return nil
}
