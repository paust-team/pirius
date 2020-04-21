package pipeline

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/message"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
)

type ConnectPipe struct {
	session *network.Session
}

func (c *ConnectPipe) Build(in ...interface{}) error {
	var ok bool
	c.session, ok = in[0].(*network.Session)
	if !ok {
		return errors.New("failed to build connect pipe")
	}
	return nil
}

func (c *ConnectPipe) Ready(ctx context.Context, inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup)(
	<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		wg.Done()
		defer close(outStream)
		defer close(errCh)

		flowed.L.Lock()
		flowed.Wait()
		flowed.L.Unlock()

		if c.session.State() != network.NONE {
			errCh <- errors.New("invalid state to connect")
			return
		}

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
				c.session.Topic().NumPubs++
			case paustq_proto.SessionType_SUBSCRIBER:
				c.session.Topic().NumSubs++
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