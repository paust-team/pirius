package service

import (
	"context"
	"github.com/paust-team/paustq/broker"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/pipeline"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	"github.com/paust-team/paustq/zookeeper"
)

type StreamService struct {
	DB       *storage.QRocksDB
	Notifier *internals.Notifier
	zKClient *zookeeper.ZKClient
	host     string
}

func NewStreamService(DB *storage.QRocksDB, notifier *internals.Notifier, zKClient *zookeeper.ZKClient,
	host string) *StreamService {
	return &StreamService{DB: DB, Notifier: notifier, zKClient: zKClient, host: host}
}

func (s *StreamService) HandleEventStreams(brokerCtx context.Context,
	eventStreams <-chan broker.EventStream) <-chan pqerror.SessionError {
	sessionErrCh := make(chan pqerror.SessionError)

	go func() {
		defer close(sessionErrCh)

		for {
			select {
			case <-brokerCtx.Done():
				return
			case eventStream := <-eventStreams:
				go s.handleEventStream(eventStream, sessionErrCh)

			}
		}
	}()

	return sessionErrCh
}

func (s *StreamService) handleEventStream(eventStream broker.EventStream, sessionErrCh chan pqerror.SessionError) {
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
		case err := <-errCh:
			pqErr, ok := err.(pqerror.PQError)
			if !ok {
				sessionErrCh <- pqerror.SessionError{
					Err:           pqerror.UnhandledError{ErrStr: err.Error()},
					Session:       session,
					CancelSession: cancelSession}
			} else {
				sessionErrCh <- pqerror.SessionError{
					Err:           pqErr,
					Session:       session,
					CancelSession: cancelSession}
			}
		}
	}
}

func convertToInterfaceChan(from <-chan *message.QMessage) chan interface{} {
	to := make(chan interface{})

	go func() {
		defer close(to)
		for msg := range from {
			to <- msg
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
		}
	}()

	return to
}

func (s *StreamService) newPipelineBase(sess *internals.Session, inlet chan interface{}) (error, *pipeline.Pipeline) {
	// build pipeline
	var dispatcher, connector, fetcher, putter, zipper pipeline.Pipe
	var err error

	dispatcher = &pipeline.DispatchPipe{}
	err = dispatcher.Build(pipeline.IsConnectRequest, pipeline.IsFetchRequest, pipeline.IsPutRequest)
	if err != nil {
		return err, nil
	}
	dispatchPipe := pipeline.NewPipe("dispatch", &dispatcher)

	connector = &pipeline.ConnectPipe{}
	err = connector.Build(sess, s.Notifier)
	if err != nil {
		return err, nil
	}
	connectPipe := pipeline.NewPipe("connect", &connector)

	fetcher = &pipeline.FetchPipe{}
	err = fetcher.Build(sess, s.DB, s.Notifier)
	if err != nil {
		return err, nil
	}
	fetchPipe := pipeline.NewPipe("fetch", &fetcher)

	putter = &pipeline.PutPipe{}
	err = putter.Build(sess, s.DB, s.zKClient, s.host)
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
	if err = pl.Add(zipPipe, connectPipe.Outlets[0], fetchPipe.Outlets[0], putPipe.Outlets[0]); err != nil {
		return err, nil
	}

	return nil, pl
}
