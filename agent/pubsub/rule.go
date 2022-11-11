package pubsub

import (
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
)

func TopicWritingRule(option topic.Option, fragments []uint) func() []uint {
	if option&topic.UniquePerFragment != 0 {
		rrSelect := helper.RoundRobinSelection(fragments)
		return func() []uint { // round-robin write for fragments if topic write policy has UniquePerFragment
			return []uint{rrSelect()}
		}
	} else {
		if len(fragments) > 1 {
			logger.Warn("This case should not be happened preferably. It's an in-efficient case because a single publisher save duplicate data.")
		}
		// else, write to all fragment
		return func() []uint {
			return fragments
		}
	}
}
