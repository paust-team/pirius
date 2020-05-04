package internals

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Subscription struct {
	TopicName         string
	LastFetchedOffset uint64
	SubscribeChan     chan bool
}

type Notifier struct {
	topicMap         sync.Map
	subscriptionChan chan *Subscription
}

func NewNotifier() *Notifier {
	return &Notifier{topicMap: sync.Map{}}
}

func (s *Notifier) LoadOrStoreTopic(topicName string) (*Topic, error) {
	value, _ := s.topicMap.LoadOrStore(topicName, NewTopic(topicName))
	topic, ok := value.(*Topic)
	if !ok {
		return nil, errors.New(fmt.Sprintf("topic(%s) not exists", topicName))
	}
	return topic, nil
}

func (s *Notifier) AddTopic(topic *Topic) {
	s.topicMap.Store(topic.Name(), topic)
}

func (s *Notifier) DeleteTopic(topicName string) {
	s.topicMap.Delete(topicName)
}

func (s *Notifier) RegisterSubscription(trigger *Subscription) {
	s.subscriptionChan <- trigger
}

func (s *Notifier) NotifyNews(ctx context.Context, errChan chan error) {
	s.subscriptionChan = make(chan *Subscription, 2)
	go func() {
		defer close(s.subscriptionChan)
		for {
			select {
			case subscription := <-s.subscriptionChan:
				if value, ok := s.topicMap.Load(subscription.TopicName); ok {
					topicData, ok := value.(*Topic)
					if !ok {
						errChan <- errors.New(fmt.Sprintf("Topic(%s) not exists", subscription.TopicName))
					}
					if subscription.LastFetchedOffset < topicData.LastOffset() {
						subscription.SubscribeChan <- true
					} else {
						s.subscriptionChan <- subscription
					}
				} else {
					errChan <- errors.New(fmt.Sprintf("Topic(%s) not exists", subscription.TopicName))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
