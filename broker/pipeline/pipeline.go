package pipeline

import (
	"context"
	"errors"
	uuid "github.com/satori/go.uuid"
	"sync"
)

type Pipeline struct {
	wg *sync.WaitGroup
	flowed *sync.Cond
	root *PipeNode
}

type PipeNode struct {
	name string
	id uuid.UUID
	pipe *Pipe
	children []*PipeNode
}

type Pipe interface {
	Build(in ...interface{}) error
}

type SourcePipe interface {
	Ready(ctx context.Context, flowed *sync.Cond, wg *sync.WaitGroup) (<-chan interface{}, <- chan error, error)
}

type SelectorPipe interface {
	AddCase(caseFn func(input interface{}) (output interface{}, ok bool))
	Ready(ctx context.Context, inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup)(
		outStreams[]chan interface{}, errCh <-chan error, err error)
}

type StreamPipe interface {
	Ready(ctx context.Context, inStream <-chan interface{}, flowed*sync.Cond, wg *sync.WaitGroup) (
		outStream<-chan interface{}, errCh <-chan error, err error)
}

type SinkPipe interface {
	Ready(ctx context.Context, inStream <-chan interface{}, flowed*sync.Cond, wg *sync.WaitGroup) (
		errCh <-chan error, err error)
}

func NewPipeNode(name string, pipe *Pipe) *PipeNode {
	return &PipeNode{
		name:     name,
		id:       uuid.NewV4(),
		pipe:     pipe,
		children: nil,
	}
}

func (p PipeNode) ID() uuid.UUID {
	return p.id
}

func (p PipeNode) Name() string {
	return p.name
}

func (p *PipeNode) AddChild(child *PipeNode) {
	p.children = append(p.children, child)
}

func NewPipeline() *Pipeline {

	return &Pipeline{
		&sync.WaitGroup{},
		sync.NewCond(&sync.Mutex{}),
		nil,
	}
}

func (p *Pipeline) FindById(id uuid.UUID) *PipeNode{
	return findById(p.root, id)
}

func findById(node *PipeNode, id uuid.UUID) *PipeNode{
	if node.id == id {
		return node
	}

	if len(node.children) > 0 {
		for _, child := range node.children {
			temp := findById(child, id)
			if temp != nil {
				return temp
			}
		}
	}

	return nil
}

func (p *Pipeline) Add(parentId uuid.UUID, child *PipeNode,
	caseFn func(input interface{}) (output interface{}, ok bool)) error {
	if p.root == nil {
		switch (*child.pipe).(type) {
		case SourcePipe:
			p.root = child
			return nil
		default:
			return errors.New("type of first pipe must be source pipe")
		}
	} else {
		if uuid.Equal(parentId, uuid.UUID{}) {
			return errors.New("parent id must be specified")
		}
		parent := p.FindById(parentId)

		switch (*parent.pipe).(type) {
		case SinkPipe:
			return errors.New("no pipe can be placed after sink pipe")
		case SelectorPipe:
			if caseFn == nil {
				return errors.New("case function must been defined if new pipe is added after selector pipe")
			}
			(*parent.pipe).(SelectorPipe).AddCase(caseFn)
			parent.AddChild(child)
			return nil
		default:
		}

		switch (*child.pipe).(type) {
		case SourcePipe:
			return errors.New("source pipe must be placed on the head of the pipeline")
		default:
			parent.AddChild(child)
			return nil
		}
	}
}

func ready(node *PipeNode, errChannels *[]<- chan error,
	ctx context.Context,
	inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup) error {
	switch (*node.pipe).(type) {
	case SinkPipe:
		errCh, err := (*node.pipe).(SinkPipe).Ready(ctx, inStream, flowed, wg)
		if err != nil {
			return err
		}
		*errChannels = append(*errChannels, errCh)
	case SourcePipe:
		outStream, errCh, err := (*node.pipe).(SourcePipe).Ready(ctx, flowed, wg)
		if err != nil {
			return err
		}
		*errChannels = append(*errChannels, errCh)
		err = ready(node.children[0], errChannels, ctx, outStream, flowed, wg)
		if err != nil {
			return err
		}
	case SelectorPipe:
		outStreams, errCh, err := (*node.pipe).(SelectorPipe).Ready(ctx, inStream, flowed, wg)
		if err != nil {
			return err
		}
		*errChannels = append(*errChannels, errCh)
		for i, child := range node.children {
			err = ready(child, errChannels, ctx, outStreams[i], flowed, wg)
			if err != nil {
				return err
			}
		}
	case StreamPipe:
		outStream, errCh, err := (*node.pipe).(StreamPipe).Ready(ctx, inStream, flowed, wg)
		if err != nil {
			return err
		}
		*errChannels = append(*errChannels, errCh)
		err =  ready(node.children[0], errChannels, ctx, outStream, flowed, wg)
		if err != nil {
			return err
		}
	default:
		return errors.New("invalid type of pipe exists" + node.Name())
	}
	return nil
}

func (p *Pipeline) Ready(ctx context.Context) ([]<-chan error, error) {
	var errChannels []<-chan error
	err := ready(p.root, &errChannels, ctx, nil, p.flowed, p.wg)
	if err != nil {
		return nil, err
	}

	return errChannels, nil
}

func (p *Pipeline) Flow() {
	go func() {
		p.wg.Wait()
		p.flowed.Broadcast()
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

