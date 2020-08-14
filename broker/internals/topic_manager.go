package internals

import (
	"github.com/paust-team/shapleq/pqerror"
	"sync"
)

type Subscription struct {
	TopicName         string
	LastFetchedOffset uint64
	SubscribeChan     chan bool
}

type TopicManager struct {
	topicMap         sync.Map
	subscriptionChan chan *Subscription
}

func NewTopicManager() *TopicManager {
	return &TopicManager{topicMap: sync.Map{}}
}

func (t *TopicManager) LoadOrStoreTopic(topicName string) (*Topic, error) {
	value, _ := t.topicMap.LoadOrStore(topicName, NewTopic(topicName))
	topic, ok := value.(*Topic)
	if !ok {
		return nil, pqerror.TopicNotExistError{Topic: topicName}
	}
	return topic, nil
}

func (t *TopicManager) AddTopic(topic *Topic) {
	t.topicMap.Store(topic.Name(), topic)
}

func (t *TopicManager) RemoveTopic(topicName string) {
	t.topicMap.Delete(topicName)
}
