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
	cb             func(event coordinator.WatchEvent)
}

func NewZKChildrenOperation(conn *zk.Conn, path string) ChildrenOperation {
	return ChildrenOperation{conn: conn, path: path}
}

func (o ChildrenOperation) WithLock(lockPath string) coordinator.ChildrenOperation {
	o.lockPath = lockPath
	return o
}

func (o ChildrenOperation) OnEvent(fn func(coordinator.WatchEvent)) coordinator.ChildrenOperation {
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
		value, _, eventCh, err = o.conn.ChildrenW(o.path)
		go func() {
			for event := range eventCh {
				var eventType coordinator.WatchEventType
				switch event.Type {
				case zk.EventNodeCreated:
					eventType = coordinator.EventNodeCreated
				case zk.EventNodeDeleted:
					eventType = coordinator.EventNodeDeleted
				case zk.EventNodeDataChanged:
					eventType = coordinator.EventNodeDataChanged
				case zk.EventNodeChildrenChanged:
					eventType = coordinator.EventNodeChildrenChanged
				default:
					log.Printf("received not defined watch-event : %+v\n", event)
				}

				if eventType != 0 {
					onEvent(coordinator.WatchEvent{
						Type: coordinator.EventNodeCreated,
						Path: event.Path,
						Err:  event.Err,
					})
				}
			}
		}()
	}

	if err != nil {
		return nil, pqerror.ZKRequestError{ZKErrStr: err.Error()}
	}

	return value, nil
}
