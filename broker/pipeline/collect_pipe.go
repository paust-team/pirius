package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"sync"
	"time"
)

type CollectPipe struct {
	session       *internals.Session
	queueForTopic map[string]chan *shapleqproto.FetchResponse
}

func (c *CollectPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	c.session, ok = in[0].(*internals.Session)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "collect"}
	}
	c.queueForTopic = map[string]chan *shapleqproto.FetchResponse{}
	return nil
}

func (c *CollectPipe) Ready(inStream <-chan interface{}) (<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)
	inStreamClosed := make(chan struct{})

	go func() {
		defer close(errCh)
		defer close(outStream)
		defer close(inStreamClosed)

		once := sync.Once{}

		for in := range inStream {
			fetchRes := in.(*shapleqproto.FetchResponse)
			once.Do(func() {
				for _, topic := range c.session.Topics() {
					if topic.MaxBatchSize() > 1 {
						c.queueForTopic[topic.TopicName()] = make(chan *shapleqproto.FetchResponse, topic.MaxBatchSize())
						go c.watchQueue(topic.TopicName(), int(topic.MaxBatchSize()), int(topic.FlushInterval()),
							inStreamClosed, outStream, errCh)
					}
				}
			})

			if queue, exists := c.queueForTopic[fetchRes.TopicName]; !exists {
				out, err := message.NewQMessageFromMsg(message.STREAM, fetchRes)
				if err != nil {
					errCh <- err
					return
				}
				outStream <- out
			} else {
				queue <- fetchRes
			}
		}
	}()

	return outStream, errCh, nil
}

func (c *CollectPipe) watchQueue(topicName string, maxBatchSize, flushInterval int, inStreamClosed chan struct{}, outStream chan interface{}, errCh chan error) {
	var collected []*shapleqproto.FetchResponse

	blockingInterval := time.Millisecond * 50
	flushIntervalMs := time.Millisecond * time.Duration(flushInterval)
	timer := time.NewTimer(flushIntervalMs)
	defer timer.Stop()

	for {
		select {
		case <-inStreamClosed:
			return
		case data := <-c.queueForTopic[topicName]:
			collected = append(collected, data)
			if len(collected) >= maxBatchSize {
				if err := c.flush(topicName, collected, outStream); err != nil {
					errCh <- err
				}
				timer.Reset(flushIntervalMs)
				collected = nil
			}
		case <-timer.C:
			if len(collected) > 0 {
				if err := c.flush(topicName, collected, outStream); err != nil {
					errCh <- err
				}
				timer.Reset(flushIntervalMs)
				collected = nil
			} else {
				// if flush time is over and no data collected,
				// then reset timer to prevent from resource starvation
				timer.Reset(blockingInterval)
			}
		}
	}
}

func (c *CollectPipe) flush(topicName string, fetchResults []*shapleqproto.FetchResponse, outStream chan interface{}) error {
	out, err := message.NewQMessageFromMsg(message.STREAM, message.NewBatchFetchResponseMsg(topicName, fetchResults))
	if err != nil {
		return err
	}
	outStream <- out

	return nil
}
