package zk_impl

import (
	"github.com/paust-team/shapleq/coordinator"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type Coordinator struct {
	zkConn *zk.Conn
}

func NewZKCoordinator(addresses []string, timeout uint, logger *logger.QLogger) (*Coordinator, error) {
	var err error
	var conn *zk.Conn
	if logger != nil {
		conn, _, err = zk.Connect(addresses, time.Millisecond*time.Duration(timeout), zk.WithLogger(logger))
	} else {
		conn, _, err = zk.Connect(addresses, time.Millisecond*time.Duration(timeout))
	}

	if err != nil {
		err = pqerror.ZKConnectionError{ZKAddrs: addresses}
		return nil, err
	}
	return &Coordinator{zkConn: conn}, nil
}

func (c Coordinator) Create(path string, value []byte) coordinator.CreateOperation {
	return NewZKCreateOperation(c.zkConn, path, value)
}

func (c Coordinator) Get(path string) coordinator.GetOperation {
	return NewZKGetOperation(c.zkConn, path)
}

func (c Coordinator) Set(path string, value []byte) coordinator.SetOperation {
	return NewZKSetOperation(c.zkConn, path, value)
}

func (c Coordinator) Delete(paths []string) coordinator.DeleteOperation {
	return NewZKDeleteOperation(c.zkConn, paths)
}

func (c Coordinator) Children(path string) coordinator.ChildrenOperation {
	return NewZKChildrenOperation(c.zkConn, path)
}

func (c Coordinator) Lock(path string, do func()) coordinator.LockOperation {
	return NewZKLockOperation(c.zkConn, path, do)
}

func (c Coordinator) OptimisticUpdate(path string, update func([]byte) []byte) coordinator.OptimisticUpdateOperation {
	return NewZKOptimisticUpdateOperation(c.zkConn, path, update)
}

func (c Coordinator) IsClosed() bool {
	return !(c.zkConn.State() == zk.StateConnecting ||
		c.zkConn.State() == zk.StateConnected ||
		c.zkConn.State() == zk.StateConnectedReadOnly ||
		c.zkConn.State() == zk.StateHasSession)
}

func (c Coordinator) Close() {
	c.zkConn.Close()
}
