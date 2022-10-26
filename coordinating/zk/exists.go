package zk

import (
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/qerror"
	"log"
)

type ExistsOperation struct {
	conn           *zk.Conn
	path, lockPath string
	cb             func(event coordinating.WatchEvent) coordinating.Recursive
}

func NewZKExistsOperation(conn *zk.Conn, path string) ExistsOperation {
	return ExistsOperation{conn: conn, path: path}
}

func (o ExistsOperation) WithLock(lockPath string) coordinating.ExistsOperation {
	o.lockPath = lockPath
	return o
}

func (o ExistsOperation) OnEvent(fn func(coordinating.WatchEvent) coordinating.Recursive) coordinating.ExistsOperation {
	o.cb = fn
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
					_, _, eventCh, err = conn.ExistsW(path)
				}
			}
		}()
	}

	if err != nil {
		return false, qerror.CoordRequestError{ErrStr: err.Error()}
	}

	return exists, nil
}
