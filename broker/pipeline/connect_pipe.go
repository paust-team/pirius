package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto/pb"
)

type ConnectPipe struct {
	session       *internals.Session
	coordiWrapper *coordinator_helper.CoordinatorWrapper
	brokerAddr    string
}

func (c *ConnectPipe) Build(in ...interface{}) error {
	casted := true
	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	coordiWrapper, ok := in[1].(*coordinator_helper.CoordinatorWrapper)
	casted = casted && ok

	brokerAddr, ok := in[2].(string)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "connect"}
	}

	c.session = session
	c.brokerAddr = brokerAddr
	c.coordiWrapper = coordiWrapper

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
			for _, topic := range req.Topics {
				if len(topic.GetTopicName()) == 0 {
					errCh <- pqerror.TopicNotSetError{}
					return
				}
				if _, err := c.coordiWrapper.GetTopicFrame(topic.GetTopicName()); err != nil {
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
				c.session.AddTopic(topic)

				switch req.SessionType {
				case shapleq_proto.SessionType_PUBLISHER:
					_, err := c.coordiWrapper.AddNumPublishers(topic.TopicName, 1)
					if err != nil {
						errCh <- err
						return
					}
					// register topic broker address only if publisher appears
					for _, fragmentOffset := range topic.Offsets {
						err = c.coordiWrapper.AddBrokerForTopic(topic.TopicName, fragmentOffset.FragmentId, c.brokerAddr)
						if err != nil {
							errCh <- err
							return
						}
					}
				case shapleq_proto.SessionType_SUBSCRIBER:
					for _, fragmentOffset := range topic.Offsets {
						_, err := c.coordiWrapper.AddNumSubscriber(topic.TopicName, fragmentOffset.FragmentId, 1)
						if err != nil {
							errCh <- err
							return
						}
					}

				default:
				}
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
