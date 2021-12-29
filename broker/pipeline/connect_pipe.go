package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/zookeeper"
	"runtime"
)

type ConnectPipe struct {
	session    *internals.Session
	zkqClient  *zookeeper.ZKQClient
	brokerAddr string
}

func (c *ConnectPipe) Build(in ...interface{}) error {
	casted := true
	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	zkqClient, ok := in[1].(*zookeeper.ZKQClient)
	casted = casted && ok

	brokerAddr, ok := in[2].(string)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "connect"}
	}

	c.session = session
	c.brokerAddr = brokerAddr
	c.zkqClient = zkqClient

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
			if _, err := c.zkqClient.GetTopicData(req.GetTopicName()); err != nil {
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
			c.session.SetTopicName(req.TopicName)

			switch req.SessionType {
			case shapleq_proto.SessionType_PUBLISHER:
				_, err := c.zkqClient.AddNumPublishers(c.session.TopicName(), 1)
				if err != nil {
					errCh <- err
					return
				}
				// register topic broker address only if publisher appears
				err = c.zkqClient.AddTopicBroker(c.session.TopicName(), c.brokerAddr)
				if err != nil {
					errCh <- err
					return
				}
			case shapleq_proto.SessionType_SUBSCRIBER:
				_, err := c.zkqClient.AddNumSubscriber(c.session.TopicName(), 1)
				if err != nil {
					errCh <- err
					return
				}
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
