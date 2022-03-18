package zk_impl

import (
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
	"log"
)

type ExistsOperation struct {
	conn           *zk.Conn
	path, lockPath string
	cb             func(event coordinator.WatchEvent) coordinator.Recursable
}

func NewZKExistsOperation(conn *zk.Conn, path string) ExistsOperation {
	return ExistsOperation{conn: conn, path: path}
}

func (o ExistsOperation) WithLock(lockPath string) coordinator.ExistsOperation {
	o.lockPath = lockPath
	return o
}

func (o ExistsOperation) OnEvent(fn func(coordinator.WatchEvent) coordinator.Recursable) coordinator.ExistsOperation {
	o.cb = fn
	return o
}

func (o ExistsOperation) Run() (bool, error) {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: o.lockPath, ZKErrStr: err.Error()}
			return false, err
		}
		defer lock.Unlock()
	}
	var exists bool
	var err error

	onEvent := o.cb
	if onEvent == nil {
		exists, _, err = o.conn.Exists(o.path)
	} else {
		var eventCh <-chan zk.Event
		path := o.path
		conn := o.conn
		exists, _, eventCh, err = conn.ExistsW(path)
		go func() {
			var reRegisterWatch coordinator.Recursable = false
			for {
				for event := range eventCh {
					switch event.Type {
					case zk.EventNodeCreated:
						reRegisterWatch = onEvent(coordinator.WatchEvent{
							Type: coordinator.EventNodeCreated,
							Path: event.Path,
							Err:  event.Err,
						})

					case zk.EventNodeDeleted:
						reRegisterWatch = onEvent(coordinator.WatchEvent{
							Type: coordinator.EventNodeDeleted,
							Path: event.Path,
							Err:  event.Err,
						})
					case zk.EventNodeDataChanged:
						reRegisterWatch = onEvent(coordinator.WatchEvent{
							Type: coordinator.EventNodeDataChanged,
							Path: event.Path,
							Err:  event.Err,
						})
					case zk.EventNodeChildrenChanged:
						reRegisterWatch = onEvent(coordinator.WatchEvent{
							Type: coordinator.EventNodeChildrenChanged,
							Path: event.Path,
							Err:  event.Err,
						})
					case zk.EventSession, zk.EventNotWatching:
						reRegisterWatch = false

					default:
						log.Printf("received not defined watch-event : %+v\n", event)
					}
				}
				if reRegisterWatch {
					_, _, eventCh, err = conn.ExistsW(path)
				}
			}
		}()
	}

	if err != nil {
		return false, pqerror.ZKRequestError{ZKErrStr: err.Error()}
	}

	return exists, nil
}
