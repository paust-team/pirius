package zk_impl

import (
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
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

func (o DeleteOperation) WithLock(lockPath string) coordinator.DeleteOperation {
	o.lockPath = lockPath
	return o
}

func (o DeleteOperation) IgnoreError() coordinator.DeleteOperation {
	o.ignoreError = true
	return o
}

func (o DeleteOperation) Run() error {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: o.lockPath, ZKErrStr: err.Error()}
			return err
		}
		defer lock.Unlock()
	}
	for _, path := range o.paths {
		if err := o.conn.Delete(path, -1); err != nil && !o.ignoreError {
			return pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
	}

	return nil
}
