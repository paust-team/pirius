package zk

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/qerror"
	"time"
)

type CoordClient struct {
	zkConn  *zk.Conn
	quorum  []string
	timeout uint
}

func NewZKCoordClient(quorum []string, timeout uint) *CoordClient {
	return &CoordClient{quorum: quorum, timeout: timeout}
}

func (c *CoordClient) Connect() error {
	conn, _, err := zk.Connect(c.quorum, time.Millisecond*time.Duration(c.timeout))

	if err != nil {
		err = qerror.CoordConnectionError{Addrs: c.quorum}
		return err
	}
	c.zkConn = conn
	return nil
}

func (c *CoordClient) Exists(path string) coordinating.ExistsOperation {
	return NewZKExistsOperation(c.zkConn, path)
}

func (c *CoordClient) Create(path string, value []byte) coordinating.CreateOperation {
	return NewZKCreateOperation(c.zkConn, path, value)
}

func (c *CoordClient) Get(path string) coordinating.GetOperation {
	return NewZKGetOperation(c.zkConn, path)
}

func (c *CoordClient) Set(path string, value []byte) coordinating.SetOperation {
	return NewZKSetOperation(c.zkConn, path, value)
}

func (c *CoordClient) Delete(paths []string) coordinating.DeleteOperation {
	return NewZKDeleteOperation(c.zkConn, paths)
}

func (c *CoordClient) Children(path string) coordinating.ChildrenOperation {
	return NewZKChildrenOperation(c.zkConn, path)
}

func (c *CoordClient) Lock(path string) coordinating.LockOperation {
	return NewZKLockOperation(c.zkConn, path)
}

func (c *CoordClient) OptimisticUpdate(path string, update func([]byte) []byte) coordinating.OptimisticUpdateOperation {
	return NewZKOptimisticUpdateOperation(c.zkConn, path, update)
}

func (c *CoordClient) IsClosed() bool {
	return !(c.zkConn.State() == zk.StateConnecting ||
		c.zkConn.State() == zk.StateConnected ||
		c.zkConn.State() == zk.StateConnectedReadOnly ||
		c.zkConn.State() == zk.StateHasSession)
}

func (c *CoordClient) Close() {
	if !c.IsClosed() {
		c.zkConn.Close()
	}
}

func ConvertToWatchEvent(event zk.Event) (coordinating.WatchEvent, error) {
	switch event.Type {
	case zk.EventNodeCreated:
		return coordinating.WatchEvent{
			Type: coordinating.EventNodeCreated,
			Path: event.Path,
			Err:  event.Err,
		}, nil
	case zk.EventNodeDeleted:
		return coordinating.WatchEvent{
			Type: coordinating.EventNodeDeleted,
			Path: event.Path,
			Err:  event.Err,
		}, nil
	case zk.EventNodeDataChanged:
		return coordinating.WatchEvent{
			Type: coordinating.EventNodeDataChanged,
			Path: event.Path,
			Err:  event.Err,
		}, nil
	case zk.EventNodeChildrenChanged:
		return coordinating.WatchEvent{
			Type: coordinating.EventNodeChildrenChanged,
			Path: event.Path,
			Err:  event.Err,
		}, nil
	case zk.EventSession, zk.EventNotWatching:
		return coordinating.WatchEvent{
			Type: coordinating.EventSession,
			Path: event.Path,
			Err:  event.Err,
		}, nil

	default:
		return coordinating.WatchEvent{}, qerror.CoordRequestError{ErrStr: fmt.Sprintf("invalid event: %+v", event)}
	}
}
