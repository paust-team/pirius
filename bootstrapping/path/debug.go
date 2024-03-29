//go:build !release

package path

const (
	HomePath       = "/qdata-debug"
	BrokersPath    = "/qdata-debug/brokers"
	TopicsPath     = "/qdata-debug/topics"
	LockPath       = "/qdata-debug/lock"
	TopicsLockPath = "/qdata-debug/lock/topics"
)
