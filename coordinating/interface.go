package coordinating

import "context"

type CoordClient interface {
	Connect() error
	IsClosed() bool
	Close()
	Exists(path string) ExistsOperation
	Create(path string, value []byte) CreateOperation
	Set(path string, value []byte) SetOperation
	Get(path string) GetOperation
	Delete(paths []string) DeleteOperation
	Children(path string) ChildrenOperation
	Lock(path string) LockOperation
	OptimisticUpdate(path string, update func(current []byte) []byte) OptimisticUpdateOperation
}

type Recursive bool

type ExistsOperation interface {
	Watchable
	WithLock(string) ExistsOperation
	Run() (bool, error)
}

type CreateOperation interface {
	WithLock(string) CreateOperation
	AsEphemeral() CreateOperation
	AsSequential() CreateOperation
	Run() error
}

type SetOperation interface {
	WithLock(string) SetOperation
	Run() error
}

type GetOperation interface {
	Watchable
	WithLock(string) GetOperation
	Run() ([]byte, error)
}

type DeleteOperation interface {
	WithLock(string) DeleteOperation
	IgnoreError() DeleteOperation
	Run() error
}

type ChildrenOperation interface {
	Watchable
	WithLock(string) ChildrenOperation
	Run() ([]string, error)
}

type LockOperation interface {
	Lock() error
	Unlock() error
}

type OptimisticUpdateOperation interface {
	Run() error
}

type Watchable interface {
	Watch(context.Context) (<-chan WatchEvent, error)
}

type WatchEventType int32

const (
	EventNodeCreated         WatchEventType = 1
	EventNodeDeleted         WatchEventType = 2
	EventNodeDataChanged     WatchEventType = 3
	EventNodeChildrenChanged WatchEventType = 4
	EventSession             WatchEventType = -1
)

type WatchEvent struct {
	Type WatchEventType
	Path string
	Err  error
}
