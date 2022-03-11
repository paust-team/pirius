package zk_impl

import (
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
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
		err = pqerror.ZKLockFailError{LockPath: o.path, ZKErrStr: err.Error()}
		return err
	}
	defer lock.Unlock()
	o.do()

	return nil
}
