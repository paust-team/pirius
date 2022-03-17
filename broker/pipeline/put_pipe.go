package pipeline

import (
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto/pb"
	"sync"
)

type PutPipe struct {
	session       *internals.Session
	db            *storage.QRocksDB
	coordiWrapper *coordinator_helper.CoordinatorWrapper
}

func (p *PutPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	session, ok := in[0].(*internals.Session)
	casted = casted && ok

	db, ok := in[1].(*storage.QRocksDB)
	casted = casted && ok

	coordiWrapper, ok := in[2].(*coordinator_helper.CoordinatorWrapper)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "put"}
	}

	p.session = session
	p.db = db
	p.coordiWrapper = coordiWrapper

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
			offsetToWrite, err := p.coordiWrapper.IncreaseLastOffset(req.TopicName, req.FragmentId)
			if err != nil {
				errCh <- err
				return
			}

			if len(req.NodeId) != 32 {
				errCh <- pqerror.InvalidNodeIdError{Id: req.NodeId}
			}
			err = p.db.PutRecord(req.TopicName, req.FragmentId, offsetToWrite, req.NodeId, req.SeqNum, req.Data)
			if err != nil {
				errCh <- err
				return
			}

			out, err := message.NewQMessageFromMsg(message.STREAM, message.NewPutResponseMsg(req.TopicName, req.FragmentId, offsetToWrite))
			if err != nil {
				errCh <- err
				return
			}

			outStream <- out
		}
	}()

	return outStream, errCh, nil
}
