package pipeline

import (
	"context"
	"github.com/paust-team/paustq/pqerror"
)

type Pipeline struct {
	Inlets      []chan interface{}
	outlets     []<-chan interface{}
	ErrChannels []<-chan error
}

type pipe struct {
	name     string
	internal *Pipe
	Outlets  []<-chan interface{}
}

type Pipe interface {
	Build(in ...interface{}) error
}

type SelectorPipe interface {
	AddCase(caseFn func(input interface{}) (output interface{}, ok bool))
	Ready(inStream <-chan interface{}) (outStreams []<-chan interface{}, errCh <-chan error, err error)
}

type VersatilePipe interface {
	Ready(inStream <-chan interface{}) (outStream <-chan interface{}, errCh <-chan error, err error)
}

type MergePipe interface {
	Ready(inStreams []<-chan interface{}) (outStream <-chan interface{}, errCh <-chan error, err error)
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
		inlets,
		nil,
		nil,
	}
}

func (p *Pipeline) Add(additive *pipe, inlets ...<-chan interface{}) error {
	var errCh <-chan error
	var err error
	var outlet <-chan interface{}

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
		additive.Outlets, errCh, err = (*additive.internal).(SelectorPipe).Ready(inlets[0])
		if err != nil {
			return err
		}
		p.ErrChannels = append(p.ErrChannels, errCh)
		for _, inlet := range inlets {
			removeIfExists(&p.outlets, inlet)
		}
		p.outlets = append(p.outlets, additive.Outlets...)
	case MergePipe:
		outlet, errCh, err = (*additive.internal).(MergePipe).Ready(inlets)
		if err != nil {
			return err
		}
		additive.Outlets = append(additive.Outlets, outlet)
		p.ErrChannels = append(p.ErrChannels, errCh)
		for _, inlet := range inlets {
			removeIfExists(&p.outlets, inlet)
		}
		p.outlets = append(p.outlets, additive.Outlets...)
	case VersatilePipe:
		outlet, errCh, err = (*additive.internal).(VersatilePipe).Ready(inlets[0])
		if err != nil {
			return err
		}
		p.ErrChannels = append(p.ErrChannels, errCh)
		additive.Outlets = append(additive.Outlets, outlet)
		for _, inlet := range inlets {
			removeIfExists(&p.outlets, inlet)
		}
		p.outlets = append(p.outlets, additive.Outlets...)
	default:
		return pqerror.InvalidPipeTypeError{PipeName: additive.Name()}
	}

	return nil
}

func (p *Pipeline) Wait(ctx context.Context) error {
	errCh := pqerror.MergeErrors(p.ErrChannels...)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-errCh:
			if !ok {
				return nil
			}
			if err != nil {
				// guarantee all pipes are done if an error occurred
				return err
			}
		}
	}
}

func (p *Pipeline) Take(outletIndex int, num int) <-chan interface{} {
	takeStream := make(chan interface{})
	go func() {
		defer close(takeStream)
		if num == 0 {
			for out := range p.outlets[outletIndex] {
				takeStream <- out
			}
		} else {
			for i := 0; i < num; i++ {
				takeStream <- <-p.outlets[outletIndex]
			}
		}
	}()

	return takeStream
}

func (p *Pipeline) Flow(inletIndex int, data ...interface{}) {
	go func() {
		for _, datum := range data {
			p.Inlets[inletIndex] <- datum
		}
	}()
}

func WaitForPipeline(ErrChannels ...<-chan error) error {
	errCh := pqerror.MergeErrors(ErrChannels...)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
