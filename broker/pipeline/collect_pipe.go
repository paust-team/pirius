package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"runtime"
	"sync"
	"time"
)

type CollectPipe struct {
	session       *internals.Session
	queueForTopic map[string]chan *shapleqproto.FetchResponse
	maxBatchSize  int
	flushInterval int64
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
		var handleFetchResponse func(response *shapleqproto.FetchResponse)

		for in := range inStream {
			fetchRes := in.(*shapleqproto.FetchResponse)
			once.Do(func() {
				c.maxBatchSize = int(c.session.MaxBatchSize())
				c.flushInterval = int64(c.session.FlushInterval())

				if c.maxBatchSize > 1 {
					for _, topic := range c.session.Topics() {
						c.queueForTopic[topic.TopicName()] = make(chan *shapleqproto.FetchResponse, c.maxBatchSize)
						go c.watchQueue(topic.TopicName(), inStreamClosed, outStream, errCh)
					}
					handleFetchResponse = func(fetchRes *shapleqproto.FetchResponse) {
						c.queueForTopic[fetchRes.TopicName] <- fetchRes
					}
				} else {
					handleFetchResponse = func(fetchRes *shapleqproto.FetchResponse) {
						out, err := message.NewQMessageFromMsg(message.STREAM, fetchRes)
						if err != nil {
							errCh <- err
							return
						}
						outStream <- out
					}
				}
			})

			handleFetchResponse(fetchRes)
		}

	}()

	return outStream, errCh, nil
}

func (c *CollectPipe) watchQueue(topicName string, inStreamClosed chan struct{}, outStream chan interface{}, errCh chan error) {
	var collected []*shapleqproto.FetchResponse
	startTime := time.Now()

	for {
		select {
		case <-inStreamClosed:
			return
		case data := <-c.queueForTopic[topicName]:
			collected = append(collected, data)
			if len(collected) >= c.maxBatchSize {
				if err := c.flush(topicName, collected, outStream); err != nil {
					startTime = time.Now()
					errCh <- err
				}
				collected = nil
			}
		default:
			if len(collected) > 0 && time.Since(startTime).Milliseconds() >= c.flushInterval {
				if err := c.flush(topicName, collected, outStream); err != nil {
					startTime = time.Now()
					errCh <- err
				}
				collected = nil
			}
		}
		runtime.Gosched()
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
