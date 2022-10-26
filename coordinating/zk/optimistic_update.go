package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/shapleq/qerror"
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
			err = qerror.CoordNoNodeError{Path: o.path}
		} else {
			err = qerror.CoordRequestError{ErrStr: err.Error()}
		}
		return err
	}

	data := o.update(value)
	_, err = o.conn.Set(o.path, data, stats.Version)

	if err != nil {
		if err == zk.ErrBadVersion {
			return o.Run() // if bad version(optimistic locked), try again
		} else {
			err = qerror.CoordRequestError{ErrStr: err.Error()}
			return err
		}
	}

	return nil
}
