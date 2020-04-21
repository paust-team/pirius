package pipeline

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/network"
	"sync"
)

type ReceivePipe struct {
	session *network.Session
}

func (r *ReceivePipe) Build(in ...interface{}) error {
	var ok bool
	r.session, ok = in[0].(*network.Session)
	if !ok {
		return errors.New("type casting failed")
	}
	return nil
}

func (r *ReceivePipe) Ready(ctx context.Context, flowed *sync.Cond, wg *sync.WaitGroup) (
	<-chan interface{}, <-chan error, error){
	errCh := make(chan error)
	outStream := make(chan interface{})

	wg.Add(1)
	go func() {
		wg.Done()
		defer close(errCh)
		defer close(outStream)

		flowed.L.Lock()
		flowed.Wait()
		flowed.L.Unlock()

		for {
			out, err := r.session.Read()
			if err != nil {
				errCh <- err
				return
			}

			// session closed
			if out == nil {
				return
			}

			select {
			case <- ctx.Done():
				return
			case outStream <- out:
			}
		}
	}()

	return outStream, errCh, nil
}
