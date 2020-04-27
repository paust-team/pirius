package pipeline

import (
	"context"
	"errors"
	"sync"
)

type Pipeline struct {
	wg *sync.WaitGroup
	inlets []chan interface{}
	outlets []<-chan interface{}
	errChannels []<-chan error
}

type pipe struct {
	name 	string
	internal *Pipe
	Outlets [] <- chan interface{}
}

type Pipe interface {
	Build(in ...interface{}) error
}

type SelectorPipe interface {
	AddCase(caseFn func(input interface{}) (output interface{}, ok bool))
	Ready(ctx context.Context, inStream <-chan interface{}, wg *sync.WaitGroup)(
		outStreams[]<-chan interface{}, errCh <-chan error, err error)
}

type VersatilePipe interface {
	Ready(ctx context.Context, inStream <-chan interface{}, wg *sync.WaitGroup) (
		outStream<-chan interface{}, errCh <-chan error, err error)
}

type MergePipe interface {
	Ready(ctx context.Context, inStreams[] <-chan interface{}, wg *sync.WaitGroup) (
		outStream <- chan interface{}, errCh <- chan error, err error)
}

func NewPipe(name string, internal *Pipe) *pipe {
	return &pipe{
		name:     name,
		internal: internal,
	}
}

func (p pipe) Name() string {
	return p.name
}

func NewPipeline(inlets ...chan interface{}) *Pipeline {
	return &Pipeline{
		&sync.WaitGroup{},
		inlets,
		nil,
		nil,
	}
}

func (p *Pipeline) Add(ctx context.Context, additive *pipe, inlets ...<- chan interface{}) error {
	var errCh <-chan error
	var err error
	var outlet <- chan interface{}

	removeIfExists := func(chs *[]<-chan interface{}, ch <-chan interface{}) {
		for i, c := range *chs {
			if c == ch {
				*chs = append((*chs)[:i], (*chs)[i+1:]...)
				break
			}
		}
	}
	switch (*additive.internal).(type) {
	case SelectorPipe:
		additive.Outlets, errCh, err = (*additive.internal).(SelectorPipe).Ready(ctx, inlets[0], p.wg)
		if err != nil {
			return err
		}
		p.errChannels = append(p.errChannels, errCh)
		for _, inlet := range inlets {
			removeIfExists(&p.outlets, inlet)
		}
		p.outlets = append(p.outlets, additive.Outlets...)
	case MergePipe:
		outlet, errCh, err = (*additive.internal).(MergePipe).Ready(ctx, inlets, p.wg)
		if err != nil {
			return err
		}
		additive.Outlets = append(additive.Outlets, outlet)
		p.errChannels = append(p.errChannels, errCh)
		for _, inlet := range inlets {
			removeIfExists(&p.outlets, inlet)
		}
		p.outlets = append(p.outlets, additive.Outlets...)
	case VersatilePipe:
		outlet, errCh, err = (*additive.internal).(VersatilePipe).Ready(ctx, inlets[0], p.wg)
		if err != nil {
			return err
		}
		p.errChannels = append(p.errChannels, errCh)
		additive.Outlets = append(additive.Outlets, outlet)
		for _, inlet := range inlets {
			removeIfExists(&p.outlets, inlet)
		}
		p.outlets = append(p.outlets, additive.Outlets...)
	default:
		return errors.New("invalid type of pipe to add")
	}

	return nil
}

func (p *Pipeline) Wait(ctx context.Context) error {
	errCh := MergeErrors(p.errChannels...)
	for {
		select {
		case <- ctx.Done():
			return nil
		case err := <- errCh:
			if err != nil {
				// guarantee all pipes are done if an error occurred
				p.wg.Wait()
				return err
			}
		}
	}
}

func (p *Pipeline) Take(ctx context.Context, outletIndex int, num int) <-chan interface{} {
	takeStream := make(chan interface{})

	go func() {
		defer close(takeStream)
		if num == 0 {
			for out := range p.outlets[outletIndex] {
				select {
				case <- ctx.Done():
					return
				case takeStream <- out:
				}
			}
		} else {
			for i := 0; i < num; i++ {
				select {
				case <- ctx.Done():
					return
				case takeStream <- <-p.outlets[outletIndex]:
				}
			}
		}
	}()
	return takeStream
}

func (p *Pipeline) Flow(ctx context.Context, inletIndex int, data ...interface{}) {
	go func() {
		for _, datum := range data {
			select {
			case <- ctx.Done():
				return
			case p.inlets[inletIndex] <- datum:
			}
		}
	}()
}

func MergeErrors(errChannels ...<-chan error) <-chan error {
	var wg sync.WaitGroup

	out := make(chan error, len(errChannels))
	output := func(c <-chan error) {
		defer wg.Done()
		for n := range c {
			out <- n
		}
	}

	wg.Add(len(errChannels))
	for _, c := range errChannels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func WaitForPipeline(errChannels ...<-chan error) error {
	errCh := MergeErrors(errChannels...)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
