package path

import "fmt"

func TopicPath(topic string) string {
	return fmt.Sprintf("%s/%s", TopicsPath, topic)
}

func TopicFragmentsPath(topic string) string {
	return fmt.Sprintf("%s/%s/fragments", TopicsPath, topic)
}

func TopicSubscriptionsPath(topic string) string {
	return fmt.Sprintf("%s/%s/subscriptions", TopicsPath, topic)
}

func TopicPubsPath(topic string) string {
	return fmt.Sprintf("%s/%s/pubs", TopicsPath, topic)
}

func TopicSubsPath(topic string) string {
	return fmt.Sprintf("%s/%s/subs", TopicsPath, topic)
}

func TopicLockPath(topic string) string {
	return fmt.Sprintf("%s/%s", TopicsLockPath, topic)
}
