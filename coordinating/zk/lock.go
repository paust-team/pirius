package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/shapleq/qerror"
)

type LockOperation struct {
	conn *zk.Conn
	path string
	do   func()
}

func NewZKLockOperation(conn *zk.Conn, path string, do func()) LockOperation {
	return LockOperation{conn: conn, path: path, do: do}
}

func (o LockOperation) Run() error {
	lock := zk.NewLock(o.conn, o.path, zk.WorldACL(zk.PermAll))
	err := lock.Lock()
	if err != nil {
		err = qerror.CoordLockFailError{LockPath: o.path, ErrStr: err.Error()}
		return err
	}
	defer lock.Unlock()
	o.do()

	return nil
}
