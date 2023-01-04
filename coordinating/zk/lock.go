package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/pirius/qerror"
)

type LockOperation struct {
	conn *zk.Conn
	path string
	lock *zk.Lock
}

func NewZKLockOperation(conn *zk.Conn, path string) LockOperation {
	return LockOperation{conn: conn, path: path, lock: zk.NewLock(conn, path, zk.WorldACL(zk.PermAll))}
}

func (o LockOperation) Lock() error {
	err := o.lock.Lock()
	if err != nil {
		err = qerror.CoordLockFailError{LockPath: o.path, ErrStr: err.Error()}
		return err
	}

	return nil
}

func (o LockOperation) Unlock() error {
	return o.lock.Unlock()
}
