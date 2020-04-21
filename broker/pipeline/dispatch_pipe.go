package pipeline

import (
	"context"
	"errors"
	"sync"
)

type DispatchPipe struct {
	cases []func(interface{}) (interface{}, bool)
}

func (d *DispatchPipe) Build(in ...interface{}) error {
	return nil
}

func (d *DispatchPipe) AddCase(caseFn func(input interface{}) (output interface{}, ok bool)) {
	d.cases = append(d.cases, caseFn)
}

func (d *DispatchPipe) Ready(ctx context.Context,
	inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup) (
	[]chan interface{}, <-chan error, error) {

	outStreams := make([]chan interface{}, len(d.cases))
	errCh := make(chan error)

	for i := range d.cases {
		outStreams[i] = make(chan interface{})
	}

	wg.Add(1)
	go func() {
		wg.Done()
		defer close(errCh)
		defer func() {
			for _, outStream := range outStreams {
				close(outStream)
			}
		}()

		flowed.L.Lock()
		flowed.Wait()
		flowed.L.Unlock()

		for in := range inStream {
			done := false
			for i, caseFn := range d.cases {
				out, ok := caseFn(in)
				if ok {
					outStreams[i] <- out
					done = true
					break
				}
			}

			if !done {
				errCh <- errors.New("message does not match with any cases")
				return
			}

			select {
			case <- ctx.Done():
				return
			default:
			}
		}
	}()

	return outStreams, errCh, nil
}
