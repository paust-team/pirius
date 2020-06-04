package pipeline

import (
	"context"
	"github.com/paust-team/paustq/pqerror"
	"sync"
	"testing"
	"time"
)

// Selector Pipe
type EvenOrOddPipe struct {
	caseCount int
	cases     []func(interface{}) (interface{}, bool)
}

func isEven(input interface{}) (interface{}, bool) {
	integer, ok := input.(int)
	if !ok {
		return nil, false
	}

	if integer%2 == 0 {
		return integer, true
	} else {
		return nil, false
	}
}

func isOdd(input interface{}) (interface{}, bool) {
	integer, ok := input.(int)
	if !ok {
		return nil, false
	}
	if integer%2 == 1 {
		return integer, true
	} else {
		return nil, false
	}
}

func (e *EvenOrOddPipe) Build(caseFns ...interface{}) error {
	e.caseCount = 0
	for _, caseFn := range caseFns {
		fn, ok := caseFn.(func(input interface{}) (output interface{}, ok bool))
		if !ok {
			return pqerror.PipeBuildFailError{PipeName: "even or odd"}
		}
		e.caseCount++
		e.AddCase(fn)
	}
	return nil
}

func (e *EvenOrOddPipe) AddCase(caseFn func(input interface{}) (output interface{}, ok bool)) {
	e.cases = append(e.cases, caseFn)
}

func (e *EvenOrOddPipe) Ready(inStream <-chan interface{}, wg *sync.WaitGroup) (
	[]<-chan interface{}, <-chan error, error) {

	if len(e.cases) != e.caseCount {
		return nil, nil, pqerror.InvalidCaseFnCountError{NumCaseFn: len(e.cases), CaseCount: e.caseCount}
	}

	outStreams := make([]chan interface{}, e.caseCount)
	errCh := make(chan error)

	for i := 0; i < e.caseCount; i++ {
		outStreams[i] = make(chan interface{})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errCh)
		defer func() {
			for _, outStream := range outStreams {
				close(outStream)
			}
		}()

		for in := range inStream {
			done := false
			for i, caseFn := range e.cases {
				out, ok := caseFn(in)
				if ok {
					outStreams[i] <- out
					done = true
					break
				}
			}

			if !done {
				errCh <- pqerror.NoCaseFnMatchError{}
				return
			}
		}
	}()

	tempStreams := make([]<-chan interface{}, e.caseCount)
	for i := 0; i < e.caseCount; i++ {
		tempStreams[i] = outStreams[i]
	}

	return tempStreams, errCh, nil
}

// Versatile Pipe
type AddPipe struct {
	additive int
}

func (a *AddPipe) Build(in ...interface{}) error {
	var ok bool
	a.additive, ok = in[0].(int)
	if !ok {
		return pqerror.PipeBuildFailError{PipeName: "add"}
	}
	return nil
}

func (a *AddPipe) Ready(inStream <-chan interface{}, wg *sync.WaitGroup) (
	<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outStream)
		defer close(errCh)

		for in := range inStream {
			outStream <- in.(int) + a.additive
		}
	}()

	return outStream, errCh, nil
}

func TestPipeline_Flow(t *testing.T) {
	inlet := make(chan interface{})
	defer close(inlet)
	pipeline := NewPipeline(inlet)

	var add1, add2, evenOrOdd, zip Pipe
	var err error

	evenOrOdd = &EvenOrOddPipe{}
	err = evenOrOdd.Build(isEven, isOdd)
	if err != nil {
		t.Error("Building even or odd pipe failed")
	}

	add1 = &AddPipe{}
	additive := 1
	err = add1.Build(additive)
	if err != nil {
		t.Error("Building add 1 pipe failed")
	}

	add2 = &AddPipe{}
	additive = 2
	err = add2.Build(additive)
	if err != nil {
		t.Error("Building add 2 pipe failed")
	}

	zip = &ZipPipe{}
	zip.Build()

	evenOrOddPipe := NewPipe("even or odd", &evenOrOdd)
	err = pipeline.Add(evenOrOddPipe, inlet)
	if err != nil {
		t.Error(err)
	}

	addPipe1 := NewPipe("add", &add1)
	err = pipeline.Add(addPipe1, evenOrOddPipe.Outlets[0])
	if err != nil {
		t.Error("Adding add pipe failed")
	}

	addPipe2 := NewPipe("add", &add2)
	err = pipeline.Add(addPipe2, evenOrOddPipe.Outlets[1])
	if err != nil {
		t.Error("Adding add pipe failed")
	}

	zipPipe := NewPipe("zip", &zip)
	err = pipeline.Add(zipPipe, addPipe1.Outlets[0], addPipe2.Outlets[0])
	if err != nil {
		t.Error("Adding even zip pipe failed")
	}

	integers := []interface{}{1, 2, 3, 4}
	pipeline.Flow(0, integers...)

	go func() {
		for out := range pipeline.Take(0, 2) {
			if out.(int) != 3 {
				t.Error("does not match expected value")
			}
		}

		for out := range pipeline.Take(0, 2) {
			if out.(int) != 5 {
				t.Error("does not match expected value")
			}
		}
	}()

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()

	err = pipeline.Wait(ctx)
	if err != nil {
		t.Error("Error occurred during flow", err)
		cancelFunc()
	}
}
