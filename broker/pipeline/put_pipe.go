package pipeline

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
	"sync/atomic"
)

type PutPipe struct {
	session *network.Session
	db      *storage.QRocksDB
}

func (p *PutPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	p.session, ok = in[0].(*network.Session)
	casted = casted && ok

	p.db, ok = in[1].(*storage.QRocksDB)
	casted = casted && ok

	if !casted {
		return errors.New("failed to build fetch pipe")
	}
	return nil
}

func (p *PutPipe) Ready(ctx context.Context, inStream <-chan interface{}, wg *sync.WaitGroup) (
	<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outStream)
		defer close(errCh)

		for in := range inStream {

			topic := p.session.Topic()

			if p.session.State() != network.ON_PUBLISH {
				err := p.session.SetState(network.ON_PUBLISH)
				if err != nil {
					errCh <- err
					return
				}
			}

			req := in.(*paustq_proto.PutRequest)

			savedOffset := atomic.AddUint64(&topic.Size, 1) - 1
			err := p.db.PutRecord(topic.Name(), savedOffset, req.Data)

			if err != nil {
				errCh <- err
				return
			}

			out, err := message.NewQMessageFromMsg(message.NewPutResponseMsg())
			if err != nil {
				errCh <- err
				return
			}

			select {
			case <-ctx.Done():
				return
			case outStream <- out:
			}
		}

	}()

	return outStream, errCh, nil
}
