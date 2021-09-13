package service

import (
	"context"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/pipeline"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper"
	"runtime"
	"sync"
)

type StreamService struct {
	DB           *storage.QRocksDB
	TopicManager *internals.TopicManager
	zKClient     *zookeeper.ZKClient
	addr         string
}

func NewStreamService(DB *storage.QRocksDB, topicManager *internals.TopicManager, zKClient *zookeeper.ZKClient,
	addr string) *StreamService {
	return &StreamService{DB: DB, TopicManager: topicManager, zKClient: zKClient, addr: addr}
}

func (s *StreamService) HandleEventStreams(brokerCtx context.Context,
	eventStreams <-chan internals.EventStream) <-chan error {
	sessionErrCh := make(chan error)

	var wg sync.WaitGroup
	go func() {
		defer func() {
			wg.Wait()
			close(sessionErrCh)
		}()

		for {
			select {
			case <-brokerCtx.Done():
				return
			case eventStream, ok := <-eventStreams:
				if ok {
					wg.Add(1)
					go s.handleEventStream(eventStream, sessionErrCh, &wg)
				}
			default:

			}
			runtime.Gosched()
		}
	}()

	return sessionErrCh
}

func (s *StreamService) handleEventStream(eventStream internals.EventStream, sessionErrCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	session := eventStream.Session
	msgCh := eventStream.MsgCh
	sessionCtx := eventStream.Ctx
	cancelSession := eventStream.CancelSession

	session.Open()
	defer session.Close()

	err, pl := s.newPipelineBase(session, convertToInterfaceChan(msgCh))

	writeErrCh, err := session.ContinuousWrite(sessionCtx,
		convertToQMessageChan(pl.Take(0, 0)))
	if err != nil {
		return
	}

	sessionErrChs := append(pl.ErrChannels, writeErrCh)
	errCh := pqerror.MergeErrors(sessionErrChs...)

	for {
		select {
		case <-sessionCtx.Done():
			return
		case err, ok := <-errCh:
			if ok {
				pqErr, ok := err.(pqerror.PQError)
				if !ok {
					sessionErrCh <- internals.SessionError{
						PQError:       pqerror.UnhandledError{ErrStr: err.Error()},
						Session:       session,
						CancelSession: cancelSession}
				} else {
					sessionErrCh <- internals.SessionError{
						PQError:       pqErr,
						Session:       session,
						CancelSession: cancelSession}
				}
			}
		default:
		}
		runtime.Gosched()
	}
}

func convertToInterfaceChan(from <-chan *message.QMessage) chan interface{} {
	to := make(chan interface{})

	go func() {
		defer close(to)
		for msg := range from {
			to <- msg
			runtime.Gosched()
		}
	}()

	return to
}

func convertToQMessageChan(from <-chan interface{}) <-chan *message.QMessage {
	to := make(chan *message.QMessage)

	go func() {
		defer close(to)
		for any := range from {
			to <- any.(*message.QMessage)
			runtime.Gosched()
		}
	}()

	return to
}

func (s *StreamService) newPipelineBase(sess *internals.Session, inlet chan interface{}) (error, *pipeline.Pipeline) {
	// build pipeline
	var dispatcher, connector, fetcher, collector, putter, zipper pipeline.Pipe
	var err error

	dispatcher = &pipeline.DispatchPipe{}
	err = dispatcher.Build(pipeline.IsConnectRequest, pipeline.IsFetchRequest, pipeline.IsPutRequest)
	if err != nil {
		return err, nil
	}
	dispatchPipe := pipeline.NewPipe("dispatch", &dispatcher)

	connector = &pipeline.ConnectPipe{}
	err = connector.Build(sess, s.TopicManager, s.zKClient, s.addr)
	if err != nil {
		return err, nil
	}
	connectPipe := pipeline.NewPipe("connect", &connector)

	fetcher = &pipeline.FetchPipe{}
	err = fetcher.Build(sess, s.DB)
	if err != nil {
		return err, nil
	}
	fetchPipe := pipeline.NewPipe("fetch", &fetcher)

	collector = &pipeline.CollectPipe{}
	err = collector.Build(sess)
	if err != nil {
		return err, nil
	}
	collectPipe := pipeline.NewPipe("collect", &collector)

	putter = &pipeline.PutPipe{}
	err = putter.Build(sess, s.DB)
	if err != nil {
		return err, nil
	}
	putPipe := pipeline.NewPipe("put", &putter)

	zipper = &pipeline.ZipPipe{}
	err = zipper.Build()
	if err != nil {
		return err, nil
	}
	zipPipe := pipeline.NewPipe("zip", &zipper)

	pl := pipeline.NewPipeline(inlet)

	if err = pl.Add(dispatchPipe, inlet); err != nil {
		return err, nil
	}
	if err = pl.Add(connectPipe, dispatchPipe.Outlets[0]); err != nil {
		return err, nil
	}
	if err = pl.Add(fetchPipe, dispatchPipe.Outlets[1]); err != nil {
		return err, nil
	}
	if err = pl.Add(putPipe, dispatchPipe.Outlets[2]); err != nil {
		return err, nil
	}
	if err = pl.Add(collectPipe, fetchPipe.Outlets[0]); err != nil {
		return err, nil
	}
	if err = pl.Add(zipPipe, connectPipe.Outlets[0], collectPipe.Outlets[0], putPipe.Outlets[0]); err != nil {
		return err, nil
	}

	return nil, pl
}
