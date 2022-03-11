package constants

import "fmt"

func GetTopicPath(topic string) string {
	return fmt.Sprintf("%s/%s", TopicsPath, topic)
}

func GetTopicFragmentPath(topic string, fragmentId uint32) string {
	return fmt.Sprintf("%s/%s/fragments/%d", TopicsPath, topic, fragmentId)
}

func GetTopicFragmentBasePath(topic string) string {
	return fmt.Sprintf("%s/%s/fragments", TopicsPath, topic)
}

func GetTopicFragmentLockPath(topic string) string {
	return fmt.Sprintf("%s/%s/fragments-lock", TopicsPath, topic)
}

func GetTopicFragmentBrokerBasePath(topic string, fragmentId uint32) string {
	return fmt.Sprintf("%s/%s/fragments/%d/brokers", TopicsPath, topic, fragmentId)
}

func GetTopicFragmentBrokerLockPath(topic string, fragmentId uint32) string {
	return fmt.Sprintf("%s/%s/fragments/%d/brokers-lock", TopicsPath, topic, fragmentId)
}
