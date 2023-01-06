package zk

import (
	"context"
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/pirius/constants"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/qerror"
	"go.uber.org/zap"
)

type ChildrenOperation struct {
	conn           *zk.Conn
	path, lockPath string
}

func NewZKChildrenOperation(conn *zk.Conn, path string) ChildrenOperation {
	return ChildrenOperation{conn: conn, path: path}
}

func (o ChildrenOperation) WithLock(lockPath string) coordinating.ChildrenOperation {
	o.lockPath = lockPath
	return o
}

func (o ChildrenOperation) Run() ([]string, error) {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = qerror.CoordLockFailError{LockPath: o.lockPath, ErrStr: err.Error()}
			return nil, err
		}
		defer lock.Unlock()
	}
	value, _, err := o.conn.Children(o.path)
	if err != nil {
		return nil, qerror.CoordRequestError{ErrStr: err.Error()}
	}

	return value, nil
}

func (o ChildrenOperation) Watch(ctx context.Context) (<-chan coordinating.WatchEvent, error) {
	_, _, eventCh, err := o.conn.ChildrenW(o.path)
	if err != nil {
		return nil, qerror.CoordRequestError{ErrStr: err.Error()}
	}

	watchCh := make(chan coordinating.WatchEvent, constants.WatchEventBuffer)
	go func() {
		defer close(watchCh)
		for {
			select {
			case <-ctx.Done():
				logger.Debug("stop watching from ctx done")
				return
			case event, ok := <-eventCh:
				if !ok {
					logger.Warn("Unexpected case occurred: stop watching from event channel closed")
					return
				}
				logger.Debug("received watching event", zap.Int32("event", int32(event.Type)), zap.String("path", event.Path))
				watchEvent, err := ConvertToWatchEvent(event)
				if err != nil {
					logger.Error("received undefined watch-event", zap.Error(err))
					return
				}
				if watchEvent.Type == coordinating.EventSession {
					logger.Warn("stop watching from EventSession") // TODO: determine error or warning
					return
				}
				watchCh <- watchEvent
				// re-register watch
				_, _, eventCh, err = o.conn.ChildrenW(o.path)
			}
		}
	}()
	return watchCh, nil
}
