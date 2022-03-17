package zk_impl

import (
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
)

type OptimisticUpdateOperation struct {
	conn   *zk.Conn
	path   string
	update func(current []byte) []byte
}

func NewZKOptimisticUpdateOperation(conn *zk.Conn, path string, update func([]byte) []byte) OptimisticUpdateOperation {
	return OptimisticUpdateOperation{conn: conn, path: path, update: update}
}

func (o OptimisticUpdateOperation) Run() error {
	value, stats, err := o.conn.Get(o.path)
	if err != nil {
		if err == zk.ErrNoNode {
			err = pqerror.ZKNoNodeError{Path: o.path}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		return err
	}

	data := o.update(value)
	_, err = o.conn.Set(o.path, data, stats.Version)

	if err != nil {
		if err == zk.ErrBadVersion {
			return o.Run() // if bad version(optimistic locked), try again
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
			return err
		}
	}

	return nil
}
