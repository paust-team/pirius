package pipeline

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/message"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
	"sync/atomic"
)

type ConnectPipe struct {
	session  *network.Session
	notifier *internals.Notifier
	zkHelper internals.ZookeeperHelper
}

func (c *ConnectPipe) Build(in ...interface{}) error {
	casted := true
	session, ok := in[0].(*network.Session)
	casted = casted && ok

	notifier, ok := in[1].(*internals.Notifier)
	casted = casted && ok

	zkHelper, ok := in[2].(internals.ZookeeperHelper)
	casted = casted && ok

	if !casted {
		return errors.New("failed to build connect pipe")
	}

	c.session = session
	c.notifier = notifier
	c.zkHelper = zkHelper

	return nil
}

func (c *ConnectPipe) Ready(ctx context.Context, inStream <-chan interface{}, wg *sync.WaitGroup) (
	<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outStream)
		defer close(errCh)

		for in := range inStream {
			req := in.(*paustq_proto.ConnectRequest)

			if c.session.State() != network.READY {
				err := c.session.SetState(network.READY)
				if err != nil {
					errCh <- err
					return
				}
			}

			c.session.SetType(req.SessionType)

			switch req.SessionType {
			case paustq_proto.SessionType_PUBLISHER:

				topic, err := c.notifier.LoadOrStoreTopic(req.TopicName)

				if err != nil {
					errCh <- err
					return
				}
				c.session.SetTopic(topic)

				atomic.AddInt64(&c.session.Topic().NumPubs, 1)

			case paustq_proto.SessionType_SUBSCRIBER:
				topicEndpoint := c.zkHelper.GetTopicEndpoint(req.TopicName)
				if topicEndpoint == "localhost" {
					topic, err := c.notifier.LoadOrStoreTopic(req.TopicName)

					if err != nil {
						errCh <- err
						return
					}
					c.session.SetTopic(topic)
					atomic.AddInt64(&c.session.Topic().NumSubs, 1)
				} else {
					c.session.SetTopic(internals.NewExternalTopic(req.TopicName, topicEndpoint))
				}

			default:
			}

			out, err := message.NewQMessageFromMsg(message.NewConnectResponseMsg())
			if err != nil {
				errCh <- err
				return
			}

			select {
			case <-ctx.Done():
				return
			case outStream <- out:
			}
		}
	}()

	return outStream, errCh, nil
}
