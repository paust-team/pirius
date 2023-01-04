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

type GetOperation struct {
	conn           *zk.Conn
	path, lockPath string
	cb             func(event coordinating.WatchEvent) coordinating.Recursive
	cbContext      context.Context
}

func NewZKGetOperation(conn *zk.Conn, path string) GetOperation {
	return GetOperation{conn: conn, path: path}
}

func (o GetOperation) WithLock(lockPath string) coordinating.GetOperation {
	o.lockPath = lockPath
	return o
}

func (o GetOperation) OnEvent(ctx context.Context, fn func(coordinating.WatchEvent) coordinating.Recursive) coordinating.GetOperation {
	o.cb = fn
	o.cbContext = ctx
	return o
}

func (o GetOperation) Run() ([]byte, error) {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = qerror.CoordLockFailError{LockPath: o.lockPath, ErrStr: err.Error()}
			return nil, err
		}
		defer lock.Unlock()
	}
	value, _, err := o.conn.Get(o.path)

	if err != nil {
		if err == zk.ErrNoNode {
			return nil, qerror.CoordNoNodeError{Path: o.path}
		} else {
			return nil, qerror.CoordRequestError{ErrStr: err.Error()}
		}
	}

	return value, nil
}

func (o GetOperation) Watch(ctx context.Context) (<-chan coordinating.WatchEvent, error) {
	_, _, eventCh, err := o.conn.GetW(o.path)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, qerror.CoordNoNodeError{Path: o.path}
		} else {
			return nil, qerror.CoordRequestError{ErrStr: err.Error()}
		}
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
				_, _, eventCh, err = o.conn.GetW(o.path)
			}
		}
	}()
	return watchCh, nil
}
