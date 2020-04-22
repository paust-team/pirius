package pipeline

import (
	"context"
	"errors"
	uuid "github.com/satori/go.uuid"
	"log"
	"sync"
	"testing"
	"time"
)

// Source Pipe
type Generator struct{
	integers []int
}

func (g *Generator) Build(in ...interface{}) error {
	var ok bool
	g.integers, ok = in[0].([]int)
	if !ok {
		return errors.New("type casting failed")
	}
	return nil
}

func (g *Generator) Ready(ctx context.Context, flowed *sync.Cond, wg *sync.WaitGroup) (
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


		for _, i := range g.integers {
			select {
			case <- ctx.Done():
				return
			case outStream <- i:
			}
		}
	}()

	return outStream, errCh, nil
}

// Stream Pipe
type Adder struct{
	additive int
}

func (a *Adder) Build(in ...interface{}) error {
	var ok bool
	a.additive, ok = in[0].(int)
	if !ok {
		return errors.New("type casting failed")
	}
	return nil
}

func (a *Adder) Ready(ctx context.Context, inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup) (
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

		for in := range inStream {
			select {
			case <- ctx.Done():
				return
			case outStream <- in.(int) + a.additive:
			}
		}
	}()


	return outStream, errCh, nil
}

// Sink Pipe
type Printer struct{
}

func (p *Printer) Build(in ...interface{}) error {
	return nil
}

func (p *Printer) Ready(ctx context.Context, inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup) (
	<-chan error, error) {
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		wg.Done()
		defer close(errCh)
		flowed.L.Lock()
		flowed.Wait()
		flowed.L.Unlock()

		for in := range inStream {
			select {
			case <- ctx.Done():
				return
			default:
				log.Println(in.(int))
			}
		}
	}()

	return errCh, nil
}


func TestPipeline_Add(t *testing.T) {
	pipeline := NewPipeline()

	var genPipe, addPipe, printPipe Pipe

	integers := []int{1, 2, 3, 4}
	genPipe = &Generator{}
	err := genPipe.Build(integers)
	generator := NewPipeNode("generator", &genPipe)

	additive := 3
	addPipe = &Adder{}
	err = addPipe.Build(additive)
	adder := NewPipeNode("adder", &addPipe)

	printPipe = &Printer{}
	printer := NewPipeNode("printer", &printPipe)

	err = pipeline.Add(uuid.UUID{}, generator, nil)
	if err != nil {
		t.Error("Adding source pipe failed")
	}

	err = pipeline.Add(generator.ID(), adder, nil)
	if err != nil {
		t.Error("Adding stream pipe failed")
	}

	err = pipeline.Add(adder.ID(), printer, nil)
	if err != nil {
		t.Error("Adding stream pipe failed")
	}
}


func TestPipeline_Flow(t *testing.T) {
	pipeline := NewPipeline()

	var genPipe, addPipe, printPipe Pipe

	integers := []int{1, 2, 3, 4}
	genPipe = &Generator{}
	err := genPipe.Build(integers)
	generator := NewPipeNode("generator",&genPipe)

	additive := 3
	addPipe = &Adder{}
	err = addPipe.Build(additive)
	adder := NewPipeNode("adder", &addPipe)

	printPipe = &Printer{}
	printer := NewPipeNode("printer", &printPipe)

	err = pipeline.Add(uuid.UUID{}, generator, nil)
	if err != nil {
		t.Error("Adding source pipe failed")
	}

	err = pipeline.Add(generator.ID(), adder, nil)
	if err != nil {
		t.Error("Adding stream pipe failed")
	}

	err = pipeline.Add(adder.ID(), printer, nil)
	if err != nil {
		t.Error("Adding stream pipe failed")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	errCh, err := pipeline.Ready(ctx)
	if err != nil {
		t.Error("Pipeline ready failed")
	}

	err = WaitForPipeline(errCh...)
	if err != nil {
		t.Error("Pipeline ready failed")
	}

	pipeline.Flow()

	time.Sleep(time.Second * 2)
}