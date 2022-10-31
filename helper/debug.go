//go:build !release

package helper

import (
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/inmemory"
)

var inMemClient *inmemory.CoordClient

func BuildCoordClient(config.AgentConfig) coordinating.CoordClient {
	if inMemClient != nil {
		return inMemClient
	}
	inMemClient = inmemory.NewInMemCoordClient()
	return inMemClient
}
