package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/qerror"
)

type DeleteOperation struct {
	conn        *zk.Conn
	paths       []string
	lockPath    string
	ignoreError bool
}

func NewZKDeleteOperation(conn *zk.Conn, paths []string) DeleteOperation {
	return DeleteOperation{conn: conn, paths: paths}
}

func (o DeleteOperation) WithLock(lockPath string) coordinating.DeleteOperation {
	o.lockPath = lockPath
	return o
}

func (o DeleteOperation) IgnoreError() coordinating.DeleteOperation {
	o.ignoreError = true
	return o
}

func (o DeleteOperation) Run() error {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = qerror.CoordLockFailError{LockPath: o.lockPath, ErrStr: err.Error()}
			return err
		}
		defer lock.Unlock()
	}
	for _, path := range o.paths {
		if err := o.conn.Delete(path, -1); err != nil && !o.ignoreError {
			return qerror.CoordRequestError{ErrStr: err.Error()}
		}
	}

	return nil
}
