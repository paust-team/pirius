package path

import (
	"fmt"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/qerror"
)

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

func TopicPublisherPath(topic string, id string) string {
	return fmt.Sprintf("%s/%s/pubs/%s", TopicsPath, topic, id)
}

func TopicSubsPath(topic string) string {
	return fmt.Sprintf("%s/%s/subs", TopicsPath, topic)
}

func TopicSubscriberPath(topic string, id string) string {
	return fmt.Sprintf("%s/%s/subs/%s", TopicsPath, topic, id)
}

func TopicLockPath(topic string) string {
	return fmt.Sprintf("%s/%s", TopicsLockPath, topic)
}

func BrokerSequentialNamePrefix() string {
	return fmt.Sprintf("%s/broker", BrokersPath)
}

func BrokerPath(name string) string {
	return fmt.Sprintf("%s/%s", BrokersPath, name)
}

func CreatePathsIfNotExist(client coordinating.CoordClient) error {
	paths := []string{HomePath, BrokersPath, TopicsPath, LockPath, TopicsLockPath}
	for _, path := range paths {
		if err := client.Create(path, []byte{}).Run(); err != nil {
			if _, ok := err.(qerror.CoordTargetAlreadyExistsError); ok { // ignore if node already exists
				continue
			}
			return err
		}
	}
	return nil
}
