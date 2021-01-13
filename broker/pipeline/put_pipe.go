package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"runtime"
	"sync"
	"sync/atomic"
)

type PutPipe struct {
	session *internals.Session
	db      *storage.QRocksDB
}

func (p *PutPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	db, ok := in[1].(*storage.QRocksDB)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "put"}
	}

	p.session = session
	p.db = db

	return nil
}

func (p *PutPipe) Ready(inStream <-chan interface{}) (<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	once := sync.Once{}
	go func() {
		defer close(errCh)
		defer close(outStream)

		for in := range inStream {
			topic := p.session.Topic()
			once.Do(func() {
				if p.session.State() != internals.ON_PUBLISH {
					err := p.session.SetState(internals.ON_PUBLISH)
					if err != nil {
						errCh <- err
						return
					}
				}
			})

			req := in.(*shapleq_proto.PutRequest)
			offset := uint64(atomic.AddInt64(&topic.Size, 1) - 1)
			err := p.db.PutRecord(topic.Name(), offset, req.Data)
			if err != nil {
				errCh <- err
				return
			}

			out, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutResponseMsg(offset))
			if err != nil {
				errCh <- err
				return
			}

			outStream <- out
			runtime.Gosched()
		}
	}()

	return outStream, errCh, nil
}
