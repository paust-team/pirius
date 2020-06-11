package pipeline

import (
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync/atomic"
)

type ConnectPipe struct {
	session  *internals.Session
	notifier *internals.Notifier
}

func (c *ConnectPipe) Build(in ...interface{}) error {
	casted := true
	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	notifier, ok := in[1].(*internals.Notifier)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "connect"}
	}

	c.session = session
	c.notifier = notifier

	return nil
}

func (c *ConnectPipe) Ready(inStream <-chan interface{}) (<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	go func() {
		defer close(outStream)
		defer close(errCh)

		for in := range inStream {
			req := in.(*paustq_proto.ConnectRequest)

			if c.session.State() != internals.READY {
				err := c.session.SetState(internals.READY)
				if err != nil {
					errCh <- err
					return
				}
			}
			c.session.SetType(req.SessionType)

			topic, err := c.notifier.LoadOrStoreTopic(req.TopicName)
			if err != nil {
				errCh <- err
				return
			}

			c.session.SetTopic(topic)

			switch req.SessionType {
			case paustq_proto.SessionType_PUBLISHER:
				atomic.AddInt64(&c.session.Topic().NumPubs, 1)
			case paustq_proto.SessionType_SUBSCRIBER:
				atomic.AddInt64(&c.session.Topic().NumSubs, 1)
			default:
			}

			out, err := message.NewQMessageFromMsg(message.STREAM, message.NewConnectResponseMsg())
			if err != nil {
				errCh <- err
				return
			}

			outStream <- out
		}
	}()

	return outStream, errCh, nil
}
