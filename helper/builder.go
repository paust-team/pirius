package helper

import (
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/coordinating/zk"
)

func BuildCoordClient(quorum []string, timeout uint) coordinating.CoordClient {
	return zk.NewZKCoordClient(quorum, timeout)
}
