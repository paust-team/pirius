package zk_impl

import (
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
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

func (o CreateOperation) WithLock(lockPath string) coordinator.CreateOperation {
	o.lockPath = lockPath
	return o
}

func (o CreateOperation) AsEphemeral() coordinator.CreateOperation {
	o.mode = o.mode | zk.FlagEphemeral
	return o
}

func (o CreateOperation) AsSequential() coordinator.CreateOperation {
	o.mode = o.mode | zk.FlagSequence
	return o
}

func (o CreateOperation) Run() error {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: o.lockPath, ZKErrStr: err.Error()}
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
			return pqerror.ZKTargetAlreadyExistsError{Target: o.path}
		} else {
			return pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
	}

	return nil
}
