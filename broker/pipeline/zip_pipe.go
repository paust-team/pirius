package pipeline

import (
	"context"
	"sync"
)

type ZipPipe struct {
}

func (z *ZipPipe) Build(in ...interface{}) error {
	return nil
}

func (z *ZipPipe) Ready(ctx context.Context, inStreams []<-chan interface{}, wg *sync.WaitGroup) (
	<-chan interface{}, <-chan error, error) {
	var waitGroup sync.WaitGroup
	outStream := make(chan interface{}, len(inStreams))
	errCh := make(chan error)

	multiplex := func(inStream <-chan interface{}) {
		defer waitGroup.Done()
		for in := range inStream {
			select {
			case <-ctx.Done():
				return
			case outStream <- in:
			}
		}
	}

	waitGroup.Add(len(inStreams))
	for _, inStream := range inStreams {
		go multiplex(inStream)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errCh)
		defer close(outStream)
		waitGroup.Wait()
	}()

	return outStream, errCh, nil
}
