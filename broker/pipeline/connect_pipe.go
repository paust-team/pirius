package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/zookeeper"
	"runtime"
	"sync/atomic"
)

type ConnectPipe struct {
	session      *internals.Session
	topicManager *internals.TopicManager
	zkClient     *zookeeper.ZKClient
	brokerAddr   string
}

func (c *ConnectPipe) Build(in ...interface{}) error {
	casted := true
	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	topicManager, ok := in[1].(*internals.TopicManager)
	casted = casted && ok

	zkClient, ok := in[2].(*zookeeper.ZKClient)
	casted = casted && ok

	brokerAddr, ok := in[3].(string)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "connect"}
	}

	c.session = session
	c.zkClient = zkClient
	c.brokerAddr = brokerAddr
	c.topicManager = topicManager

	return nil
}

func (c *ConnectPipe) Ready(inStream <-chan interface{}) (<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	go func() {
		defer close(errCh)
		defer close(outStream)

		for in := range inStream {
			req := in.(*shapleq_proto.ConnectRequest)
			if len(req.GetTopicName()) == 0 {
				errCh <- pqerror.TopicNotSetError{}
				return
			}
			if _, err := c.zkClient.GetTopic(req.GetTopicName()); err != nil {
				errCh <- err
				return
			}

			if c.session.State() != internals.READY {
				err := c.session.SetState(internals.READY)
				if err != nil {
					errCh <- err
					return
				}
			}
			c.session.SetType(req.SessionType)

			topic, err := c.topicManager.LoadOrStoreTopic(req.TopicName)
			if err != nil {
				errCh <- err
				return
			}

			c.session.SetTopic(topic)

			switch req.SessionType {
			case shapleq_proto.SessionType_PUBLISHER:
				atomic.AddInt64(&c.session.Topic().NumPubs, 1)
				err := c.zkClient.AddTopicBroker(c.session.Topic().Name(), c.brokerAddr)
				if err != nil {
					errCh <- err
					return
				}
			case shapleq_proto.SessionType_SUBSCRIBER:
				atomic.AddInt64(&c.session.Topic().NumSubs, 1)
			default:
			}

			out, err := message.NewQMessageFromMsg(message.STREAM, message.NewConnectResponseMsg())
			if err != nil {
				errCh <- err
				return
			}

			outStream <- out
			runtime.Gosched()
		}
	}()

	return outStream, errCh, nil
}
