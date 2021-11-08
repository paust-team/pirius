package internals

import (
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper"
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
	zkClient         *zookeeper.ZKClient
}

func NewTopicManager(zkClient *zookeeper.ZKClient) *TopicManager {
	return &TopicManager{topicMap: sync.Map{}, zkClient: zkClient}
}

func (t *TopicManager) LoadOrStoreTopic(topicName string) (*Topic, error) {
	value, loaded := t.topicMap.Load(topicName)
	if !loaded {
		if topicMeta, err := t.zkClient.GetTopicData(topicName); err == nil {
			topic := NewTopic(topicName, topicMeta)
			t.topicMap.Store(topicName, topic)
			return topic, nil
		}
		return nil, pqerror.TopicNotExistError{Topic: topicName}
	} else {
		topic, ok := value.(*Topic)
		if !ok {
			return nil, pqerror.TopicNotExistError{Topic: topicName}
		}
		return topic, nil
	}
}
