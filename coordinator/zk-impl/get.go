package zk_impl

import (
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
	"log"
)

type GetOperation struct {
	conn           *zk.Conn
	path, lockPath string
	cb             func(event coordinator.WatchEvent)
}

func NewZKGetOperation(conn *zk.Conn, path string) GetOperation {
	return GetOperation{conn: conn, path: path}
}

func (o GetOperation) WithLock(lockPath string) coordinator.GetOperation {
	o.lockPath = lockPath
	return o
}

func (o GetOperation) OnEvent(fn func(coordinator.WatchEvent)) coordinator.GetOperation {
	o.cb = fn
	return o
}

func (o GetOperation) Run() ([]byte, error) {
	if o.lockPath != "" {
		lock := zk.NewLock(o.conn, o.lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: o.lockPath, ZKErrStr: err.Error()}
			return nil, err
		}
		defer lock.Unlock()
	}
	var value []byte
	var err error

	onEvent := o.cb
	if onEvent == nil {
		value, _, err = o.conn.Get(o.path)

	} else {
		var eventCh <-chan zk.Event
		value, _, eventCh, err = o.conn.GetW(o.path)
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
		if err == zk.ErrNoNode {
			return nil, pqerror.ZKNoNodeError{Path: o.path}
		} else {
			return nil, pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
	}

	return value, nil
}
