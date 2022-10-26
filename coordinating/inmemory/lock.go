package inmemory

import "github.com/paust-team/shapleq/agent/logger"

type LockOperation struct {
	do func()
}

func NewInMemLockOperation(do func()) LockOperation {
	return LockOperation{do: do}
}

func (o LockOperation) Run() error {
	logger.Warn("not fully implement in in-mem coordinator")
	o.do()
	return nil
}
