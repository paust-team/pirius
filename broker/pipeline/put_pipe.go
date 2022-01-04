package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
	"runtime"
	"sync"
)

type PutPipe struct {
	session   *internals.Session
	db        *storage.QRocksDB
	zkqClient *zookeeper.ZKQClient
}

func (p *PutPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	db, ok := in[1].(*storage.QRocksDB)
	casted = casted && ok

	zkqClient, ok := in[2].(*zookeeper.ZKQClient)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "put"}
	}

	p.session = session
	p.db = db
	p.zkqClient = zkqClient

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
			topicName := p.session.TopicName()
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
			offsetToWrite, err := p.zkqClient.IncreaseLastOffset(topicName)
			if err != nil {
				errCh <- err
				return
			}

			if len(req.NodeId) != 32 {
				errCh <- pqerror.InvalidNodeIdError{Id: req.NodeId}
			}
			err = p.db.PutRecord(topicName, offsetToWrite, req.NodeId, req.SeqNum, req.Data)
			if err != nil {
				errCh <- err
				return
			}

			out, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutResponseMsg(offsetToWrite))
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
