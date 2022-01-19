//go:build !release
// +build !release

package constants

const (
	ShapleQPath            = "/shapleq-debug"
	BrokersPath            = "/shapleq-debug/brokers"
	TopicsPath             = "/shapleq-debug/topics"
	BrokersLockPath        = "/shapleq-debug/brokers-lock"
	TopicsLockPath         = "/shapleq-debug/topics-lock"
	TopicFragmentsLockPath = "/shapleq-debug/topic-fragments-lock"
)
