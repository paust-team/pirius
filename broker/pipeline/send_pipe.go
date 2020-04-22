package pipeline

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/message"
	"sync"
)

type SendPipe struct {
	session *network.Session
}

func (s *SendPipe) Build(in ...interface{}) error {
	var ok bool
	s.session, ok = in[0].(*network.Session)
	if !ok {
		return errors.New("type casting failed")
	}
	return nil
}

func (s *SendPipe) Ready(ctx context.Context, inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup) (
	<-chan error, error){
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		wg.Done()
		defer close(errCh)

		flowed.L.Lock()
		flowed.Wait()
		flowed.L.Unlock()

		for in := range inStream {
			res := in.(*message.QMessage)
			err := s.session.Write(res, 1024)
			if err != nil {
				errCh <- err
				return
			}

			select {
			case <- ctx.Done():
				return
			default:
			}
		}
	}()

	return errCh, nil
}