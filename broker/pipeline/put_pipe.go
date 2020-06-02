package pipeline

import (
	"context"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustq_proto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"sync"
	"sync/atomic"
)

type PutPipe struct {
	session     *internals.Session
	db          *storage.QRocksDB
	zkClient    *zookeeper.ZKClient
	host        string
	brokerAdded bool
}

func (p *PutPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	db, ok := in[1].(*storage.QRocksDB)
	casted = casted && ok

	zkClient, ok := in[2].(*zookeeper.ZKClient)
	casted = casted && ok

	host, ok := in[3].(string)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "put"}
	}

	p.session = session
	p.db = db
	p.zkClient = zkClient
	p.host = host
	p.brokerAdded = false

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
			if p.session.State() != internals.ON_PUBLISH {
				err := p.session.SetState(internals.ON_PUBLISH)
				if err != nil {
					errCh <- err
					return
				}
			}

			req := in.(*paustq_proto.PutRequest)
			if !p.brokerAdded {
				err := p.zkClient.AddTopicBroker(p.session.Topic().Name(), p.host)
				if err != nil {
					errCh <- err
					return
				}
				p.brokerAdded = true
			}

			savedOffset := uint64(atomic.AddInt64(&topic.Size, 1) - 1)
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
