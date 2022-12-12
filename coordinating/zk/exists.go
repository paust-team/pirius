package zk

import (
	"context"
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
)

type ExistsOperation struct {
	conn           *zk.Conn
	path, lockPath string
}

func NewZKExistsOperation(conn *zk.Conn, path string) ExistsOperation {
	return ExistsOperation{conn: conn, path: path}
}

func (o ExistsOperation) WithLock(lockPath string) coordinating.ExistsOperation {
	o.lockPath = lockPath
	return o
}

func (o ExistsOperation) Run() (bool, error) {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = qerror.CoordLockFailError{LockPath: o.lockPath, ErrStr: err.Error()}
			return false, err
		}
		defer lock.Unlock()
	}
	exists, _, err := o.conn.Exists(o.path)
	if err != nil {
		return false, qerror.CoordRequestError{ErrStr: err.Error()}
	}
	return exists, nil
}

func (o ExistsOperation) Watch(ctx context.Context) (<-chan coordinating.WatchEvent, error) {
	_, _, eventCh, err := o.conn.ExistsW(o.path)
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
				_, _, eventCh, err = o.conn.ExistsW(o.path)
			}
		}
	}()
	return watchCh, nil
}
