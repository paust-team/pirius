package zk_impl

import (
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
	"log"
)

type ChildrenOperation struct {
	conn           *zk.Conn
	path, lockPath string
	cb             func(event coordinator.WatchEvent) coordinator.Recursable
}

func NewZKChildrenOperation(conn *zk.Conn, path string) ChildrenOperation {
	return ChildrenOperation{conn: conn, path: path}
}

func (o ChildrenOperation) WithLock(lockPath string) coordinator.ChildrenOperation {
	o.lockPath = lockPath
	return o
}

func (o ChildrenOperation) OnEvent(fn func(coordinator.WatchEvent) coordinator.Recursable) coordinator.ChildrenOperation {
	o.cb = fn
	return o
}

func (o ChildrenOperation) Run() ([]string, error) {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: o.lockPath, ZKErrStr: err.Error()}
			return nil, err
		}
		defer lock.Unlock()
	}
	var value []string
	var err error

	onEvent := o.cb
	if onEvent == nil {
		value, _, err = o.conn.Children(o.path)
	} else {
		var eventCh <-chan zk.Event
		path := o.path
		conn := o.conn
		_, _, eventCh, err = conn.ChildrenW(path)
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
					_, _, eventCh, err = conn.ChildrenW(path)
				}
			}
		}()
	}

	if err != nil {
		return nil, pqerror.ZKRequestError{ZKErrStr: err.Error()}
	}

	return value, nil
}
