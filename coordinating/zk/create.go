package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/qerror"
)

type CreateOperation struct {
	conn           *zk.Conn
	path, lockPath string
	value          []byte
	mode           int32
}

func NewZKCreateOperation(conn *zk.Conn, path string, value []byte) CreateOperation {
	return CreateOperation{conn: conn, path: path, value: value}
}

func (o CreateOperation) WithLock(lockPath string) coordinating.CreateOperation {
	o.lockPath = lockPath
	return o
}

func (o CreateOperation) AsEphemeral() coordinating.CreateOperation {
	o.mode = o.mode | zk.FlagEphemeral
	return o
}

func (o CreateOperation) AsSequential() coordinating.CreateOperation {
	o.mode = o.mode | zk.FlagSequence
	return o
}

func (o CreateOperation) Run() error {
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
	if o.mode == zk.FlagEphemeral|zk.FlagSequence {
		_, err = o.conn.CreateProtectedEphemeralSequential(o.path, o.value, zk.WorldACL(zk.PermAll))
	} else {
		_, err = o.conn.Create(o.path, o.value, o.mode, zk.WorldACL(zk.PermAll))
	}

	if err != nil {
		if err == zk.ErrNodeExists {
			return qerror.CoordTargetAlreadyExistsError{Target: o.path}
		} else {
			return qerror.CoordRequestError{ErrStr: err.Error()}
		}
	}

	return nil
}
