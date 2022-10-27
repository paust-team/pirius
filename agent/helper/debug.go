//go:build !release

package helper

import (
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/inmemory"
)

func BuildCoordClient(config.AgentConfig) coordinating.CoordClient {
	return inmemory.NewInMemCoordClient()
}
