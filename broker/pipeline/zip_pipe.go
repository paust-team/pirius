package pipeline

import (
	"sync"
)

type ZipPipe struct {
}

func (z *ZipPipe) Build(in ...interface{}) error {
	return nil
}

func (z *ZipPipe) Ready(inStreams []<-chan interface{}) (<-chan interface{}, <-chan error, error) {
	var waitGroup sync.WaitGroup
	outStream := make(chan interface{}, len(inStreams))
	errCh := make(chan error)

	multiplex := func(inStream <-chan interface{}) {
		defer waitGroup.Done()
		for in := range inStream {
			outStream <- in
		}
	}

	waitGroup.Add(len(inStreams))
	for _, inStream := range inStreams {
		go multiplex(inStream)
	}

	go func() {
		defer close(errCh)
		defer close(outStream)
		waitGroup.Wait()
	}()

	return outStream, errCh, nil
}
