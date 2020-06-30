package internals

import (
	"context"
	"github.com/paust-team/shapleq/pqerror"
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
		return nil, pqerror.TopicNotExistError{Topic: topicName}
	}
	return topic, nil
}

func (s *Notifier) AddTopic(topic *Topic) {
	s.topicMap.Store(topic.Name(), topic)
}

func (s *Notifier) RemoveTopic(topicName string) {
	s.topicMap.Delete(topicName)
}

func (s *Notifier) RegisterSubscription(trigger *Subscription) {
	s.subscriptionChan <- trigger
}

func (s *Notifier) NotifyNews(ctx context.Context) <-chan error {
	s.subscriptionChan = make(chan *Subscription, 2)
	errCh := make(chan error)
	go func() {
		defer close(s.subscriptionChan)
		defer close(errCh)
		for {
			select {
			case subscription := <-s.subscriptionChan:
				if value, ok := s.topicMap.Load(subscription.TopicName); ok {
					topicData := value.(*Topic)
					if subscription.LastFetchedOffset < topicData.LastOffset() {
						subscription.SubscribeChan <- true
					} else {
						s.subscriptionChan <- subscription
					}
				} else {
					errCh <- pqerror.NewTopicNotExistError(subscription.TopicName)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return errCh
}
