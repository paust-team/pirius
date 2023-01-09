package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/qerror"
)

type SetOperation struct {
	conn           *zk.Conn
	path, lockPath string
	value          []byte
}

func NewZKSetOperation(conn *zk.Conn, path string, value []byte) SetOperation {
	return SetOperation{conn: conn, path: path, value: value}
}

func (o SetOperation) WithLock(lockPath string) coordinating.SetOperation {
	o.lockPath = lockPath
	return o
}

func (o SetOperation) Run() error {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = qerror.CoordLockFailError{LockPath: o.lockPath, ErrStr: err.Error()}
			return err
		}
		defer lock.Unlock()
	}
	var err error

	_, err = o.conn.Set(o.path, o.value, -1)

	if err != nil {
		return qerror.CoordRequestError{ErrStr: err.Error()}
	}

	return nil
}
