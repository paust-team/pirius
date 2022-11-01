//go:build release

package helper

import (
	"github.com/paust-team/shapleq/coordinating/zk"
)

func BuildCoordClient(quorum []string, timeout uint) coordinating.CoordClient {
	return zk.NewZKCoordClient(quorum, timeout)
}
