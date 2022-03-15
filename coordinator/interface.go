package coordinator

type Coordinator interface {
	IsClosed() bool
	Close()
	Create(path string, value []byte) CreateOperation
	Set(path string, value []byte) SetOperation
	Get(path string) GetOperation
	Delete(paths []string) DeleteOperation
	Children(path string) ChildrenOperation
	Lock(path string, do func()) LockOperation
	OptimisticUpdate(path string, update func(current []byte) []byte) OptimisticUpdateOperation
}

type Recursable bool

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
	WithLock(string) GetOperation
	OnEvent(func(WatchEvent) Recursable) GetOperation
	Run() ([]byte, error)
}

type DeleteOperation interface {
	WithLock(string) DeleteOperation
	IgnoreError() DeleteOperation
	Run() error
}

type ChildrenOperation interface {
	WithLock(string) ChildrenOperation
	OnEvent(func(WatchEvent) Recursable) ChildrenOperation
	Run() ([]string, error)
}

type LockOperation interface {
	Run() error
}

type OptimisticUpdateOperation interface {
	Run() error
}

type WatchEventType int32

const (
	EventNodeCreated         WatchEventType = 1
	EventNodeDeleted         WatchEventType = 2
	EventNodeDataChanged     WatchEventType = 3
	EventNodeChildrenChanged WatchEventType = 4
)

type WatchEvent struct {
	Type WatchEventType
	Path string
	Err  error
}
