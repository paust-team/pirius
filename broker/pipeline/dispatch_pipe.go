package pipeline

import (
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"runtime"
)

func IsConnectRequest(data interface{}) (interface{}, bool) {
	msg, ok := data.(*message.QMessage)
	if !ok {
		return nil, false
	}
	pb := &shapleqproto.ConnectRequest{}
	err := msg.UnpackTo(pb)
	if err != nil {
		return nil, false
	}
	return pb, true
}

func IsFetchRequest(data interface{}) (interface{}, bool) {
	msg, ok := data.(*message.QMessage)
	if !ok {
		return nil, false
	}
	pb := &shapleqproto.FetchRequest{}
	err := msg.UnpackTo(pb)
	if err != nil {
		return nil, false
	}
	return pb, true
}

func IsPutRequest(data interface{}) (interface{}, bool) {
	msg, ok := data.(*message.QMessage)
	if !ok {
		return nil, false
	}
	pb := &shapleqproto.PutRequest{}
	err := msg.UnpackTo(pb)
	if err != nil {
		return nil, false
	}
	return pb, true
}

type DispatchPipe struct {
	caseCount int
	cases     []func(interface{}) (interface{}, bool)
}

func (d *DispatchPipe) Build(caseFns ...interface{}) error {
	d.caseCount = 0
	for _, caseFn := range caseFns {
		fn, ok := caseFn.(func(input interface{}) (output interface{}, ok bool))
		if !ok {
			return pqerror.PipeBuildFailError{PipeName: "dispatch"}
		}
		d.caseCount++
		d.AddCase(fn)
	}
	return nil
}

func (d *DispatchPipe) AddCase(caseFn func(input interface{}) (output interface{}, ok bool)) {
	d.cases = append(d.cases, caseFn)
}

func (d *DispatchPipe) Ready(inStream <-chan interface{}) ([]<-chan interface{}, <-chan error, error) {

	if len(d.cases) != d.caseCount {
		return nil, nil, pqerror.InvalidCaseFnCountError{NumCaseFn: len(d.cases), CaseCount: d.caseCount}
	}

	outStreams := make([]chan interface{}, d.caseCount)
	errCh := make(chan error)

	for i := 0; i < d.caseCount; i++ {
		outStreams[i] = make(chan interface{})
	}

	go func() {
		defer close(errCh)
		defer func() {
			for _, outStream := range outStreams {
				close(outStream)
			}
		}()

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
				errCh <- pqerror.InvalidMsgTypeToUnpackError{}
				return
			}
			runtime.Gosched()
		}
	}()

	// cast the out channels to receive channels
	tempStreams := make([]<-chan interface{}, d.caseCount)
	for i := 0; i < d.caseCount; i++ {
		tempStreams[i] = outStreams[i]
	}

	return tempStreams, errCh, nil
}
