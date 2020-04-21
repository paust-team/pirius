package pipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
)

type PutPipe struct {
	session *network.Session
	db *storage.QRocksDB
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

func (p *PutPipe) Ready(ctx context.Context, inStream <-chan interface{}, flowed *sync.Cond, wg *sync.WaitGroup)(
	<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	fmt.Println("put pipe ready")
	fmt.Println(inStream)
	wg.Add(1)
	go func() {
		wg.Done()
		defer close(outStream)
		defer close(errCh)

		flowed.L.Lock()
		flowed.Wait()
		flowed.L.Unlock()

		for in := range inStream {
			if p.session.State() != network.ON_PUBLISH {
				err := p.session.SetState(network.ON_PUBLISH)
				if err != nil {
					errCh <- err
					return
				}
			}

			req := in.(*paustq_proto.PutRequest)
			topic := p.session.Topic()
			err := p.db.PutRecord(topic.Name(), topic.Size, req.Data)
			topic.Size++

			if err != nil {
				errCh <- err
				return
			}
			topic.NotifyPublished()

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