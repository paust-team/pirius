//go:build !release

package helper

import (
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/inmemory"
)

var inMemClient *inmemory.CoordClient

func BuildCoordClient([]string, uint) coordinating.CoordClient {
	if inMemClient != nil {
		return inMemClient
	}
	inMemClient = inmemory.NewInMemCoordClient()
	return inMemClient
}
