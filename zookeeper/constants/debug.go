// +build !release

package constants

const (
	SHAPLEQ            ZKPath = "/sq-debug/shapleq"
	BROKERS            ZKPath = "/sq-debug/shapleq/brokers"
	TOPICS             ZKPath = "/sq-debug/shapleq/topics"
	TOPIC_BROKERS      ZKPath = "/sq-debug/shapleq/topic-brokers"
	BROKERS_LOCK       ZKPath = "/sq-debug/brokers-lock"
	TOPICS_LOCK        ZKPath = "/sq-debug/topics-lock"
	TOPIC_BROKERS_LOCK ZKPath = "/sq-debug/topic-brokers-lock"
)
