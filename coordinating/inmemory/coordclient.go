package inmemory

import (
	"github.com/paust-team/pirius/coordinating"
	"sync"
)

const ephemeralPath = "eps"

type CoordClient struct {
	m      sync.Map
	closed bool
}

func NewInMemCoordClient() *CoordClient {
	return &CoordClient{m: sync.Map{}, closed: false}
}

func (c *CoordClient) Connect() error {
	c.m.Store(ephemeralPath, []string{})
	return nil
}

func (c *CoordClient) Exists(path string) coordinating.ExistsOperation {
	return NewInMemExistsOperation(&c.m, path)
}

func (c *CoordClient) Create(path string, value []byte) coordinating.CreateOperation {
	return NewInMemCreateOperation(&c.m, path, value)
}

func (c *CoordClient) Get(path string) coordinating.GetOperation {
	return NewInMemGetOperation(&c.m, path)
}

func (c *CoordClient) Set(path string, value []byte) coordinating.SetOperation {
	return NewInMemSetOperation(&c.m, path, value)
}

func (c *CoordClient) Delete(paths []string) coordinating.DeleteOperation {
	return NewInMemDeleteOperation(&c.m, paths)
}

func (c *CoordClient) Children(path string) coordinating.ChildrenOperation {
	return NewInMemChildrenOperation(&c.m, path)
}

func (c *CoordClient) Lock(path string) coordinating.LockOperation {
	return NewInMemLockOperation()
}

func (c *CoordClient) OptimisticUpdate(path string, update func([]byte) []byte) coordinating.OptimisticUpdateOperation {
	return NewInMemOptimisticUpdateOperation(&c.m, path, update)
}

func (c *CoordClient) IsClosed() bool {
	return c.closed
}

func (c *CoordClient) Close() {
	c.closed = true
	epPaths, _ := c.m.Load(ephemeralPath)
	for _, path := range epPaths.([]string) {
		c.m.Delete(path)
	}
}
