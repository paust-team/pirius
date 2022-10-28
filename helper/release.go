//go:build release

package helper

import (
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/coordinating/zk"
)

func BuildCoordClient(cfg config.AgentConfig) coordinating.CoordClient {
	return zk.NewZKCoordClient(cfg.ZKQuorum(), cfg.ZKTimeout())
}
