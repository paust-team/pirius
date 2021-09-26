package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"runtime"
	"sync"
	"time"
)

type CollectPipe struct {
	session       *internals.Session
	queue         chan *shapleqproto.FetchResponse
	startTime     time.Time
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
			once.Do(func() {
				c.maxBatchSize = int(c.session.MaxBatchSize())
				c.flushInterval = int64(c.session.FlushInterval())
				c.startTime = time.Now()

				if c.maxBatchSize > 1 {
					c.queue = make(chan *shapleqproto.FetchResponse, c.maxBatchSize)
					go c.watchQueue(inStreamClosed, outStream, errCh)
					handleFetchResponse = func(fetchRes *shapleqproto.FetchResponse) {
						c.queue <- fetchRes
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

			handleFetchResponse(in.(*shapleqproto.FetchResponse))
		}

	}()

	return outStream, errCh, nil
}

func (c *CollectPipe) watchQueue(inStreamClosed chan struct{}, outStream chan interface{}, errCh chan error) {
	var collected []*shapleqproto.FetchResponse

	for {
		select {
		case <-inStreamClosed:
			return
		case data := <-c.queue:
			collected = append(collected, data)
			if len(collected) >= c.maxBatchSize {
				if err := c.flush(collected, outStream); err != nil {
					errCh <- err
				}
				collected = nil
			}
		default:
			if time.Since(c.startTime).Milliseconds() >= c.flushInterval {
				if err := c.flush(collected, outStream); err != nil {
					errCh <- err
				}
				collected = nil
			}
		}
		runtime.Gosched()
	}
}

func (c *CollectPipe) flush(fetchResults []*shapleqproto.FetchResponse, outStream chan interface{}) error {
	c.startTime = time.Now()

	out, err := message.NewQMessageFromMsg(message.STREAM, message.NewBatchFetchResponseMsg(fetchResults))
	if err != nil {
		return err
	}
	outStream <- out

	return nil
}
