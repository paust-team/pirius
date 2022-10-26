package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/qerror"
	"log"
)

type GetOperation struct {
	conn           *zk.Conn
	path, lockPath string
	cb             func(event coordinating.WatchEvent) coordinating.Recursive
}

func NewZKGetOperation(conn *zk.Conn, path string) GetOperation {
	return GetOperation{conn: conn, path: path}
}

func (o GetOperation) WithLock(lockPath string) coordinating.GetOperation {
	o.lockPath = lockPath
	return o
}

func (o GetOperation) OnEvent(fn func(coordinating.WatchEvent) coordinating.Recursive) coordinating.GetOperation {
	o.cb = fn
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
	var value []byte
	var err error

	onEvent := o.cb
	if onEvent == nil {
		value, _, err = o.conn.Get(o.path)

	} else {
		var eventCh <-chan zk.Event
		path := o.path
		conn := o.conn
		value, _, eventCh, err = conn.GetW(path)
		go func() {
			var reRegisterWatch coordinating.Recursive = false
			for {
				for event := range eventCh {
					switch event.Type {
					case zk.EventNodeCreated:
						reRegisterWatch = onEvent(coordinating.WatchEvent{
							Type: coordinating.EventNodeCreated,
							Path: event.Path,
							Err:  event.Err,
						})

					case zk.EventNodeDeleted:
						reRegisterWatch = onEvent(coordinating.WatchEvent{
							Type: coordinating.EventNodeDeleted,
							Path: event.Path,
							Err:  event.Err,
						})
					case zk.EventNodeDataChanged:
						reRegisterWatch = onEvent(coordinating.WatchEvent{
							Type: coordinating.EventNodeDataChanged,
							Path: event.Path,
							Err:  event.Err,
						})
					case zk.EventNodeChildrenChanged:
						reRegisterWatch = onEvent(coordinating.WatchEvent{
							Type: coordinating.EventNodeChildrenChanged,
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
					_, _, eventCh, err = conn.GetW(path)
				}
			}
		}()
	}

	if err != nil {
		if err == zk.ErrNoNode {
			return nil, qerror.CoordNoNodeError{Path: o.path}
		} else {
			return nil, qerror.CoordRequestError{ErrStr: err.Error()}
		}
	}

	return value, nil
}
