package service

import (
	"context"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/pipeline"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	"github.com/paust-team/paustq/zookeeper"
	"sync"
)

type StreamService struct {
	DB       *storage.QRocksDB
	Notifier *internals.Notifier
	zKClient *zookeeper.ZKClient
	host     string
	sessions Sessions
}

type Sessions struct {
	sync.Mutex
	ss []*internals.Session
}

type BrokerStoppableErrMsg struct {
	Err           pqerror.PQError
	ClientVisible bool
	Broadcastable bool
}

type SessionCloseableErrMsg struct {
	Err           pqerror.PQError
	CancelSession context.CancelFunc
	Session       *internals.Session
	ClientVisible bool
	Broadcastable bool
}

type SustainableErrMsg struct {
	Err           pqerror.PQError
	Session       *internals.Session
	ClientVisible bool
	Broadcastable bool
}

func NewStreamService(DB *storage.QRocksDB, notifier *internals.Notifier, zKClient *zookeeper.ZKClient,
	host string) *StreamService {
	return &StreamService{DB: DB, Notifier: notifier, zKClient: zKClient, host: host}
}

func (s *StreamService) HandleNewSessions(brokerCtx context.Context, sessionCh <-chan *internals.Session) (
	chan BrokerStoppableErrMsg,
	chan SessionCloseableErrMsg,
	chan SustainableErrMsg) {

	brokerStoppableErrCh := make(chan BrokerStoppableErrMsg)
	sessionCloseableErrCh := make(chan SessionCloseableErrMsg)
	sustainableErrCh := make(chan SustainableErrMsg)

	go func() {
		defer close(brokerStoppableErrCh)
		defer close(sessionCloseableErrCh)
		defer close(sustainableErrCh)

		for session := range sessionCh {
			go s.handleNewSession(brokerCtx, session, brokerStoppableErrCh, sessionCloseableErrCh, sustainableErrCh)
		}
	}()

	return brokerStoppableErrCh, sessionCloseableErrCh, sustainableErrCh
}

func (s *StreamService) addSession(session *internals.Session) {
	s.sessions.Lock()
	defer s.sessions.Unlock()
	s.sessions.ss = append(s.sessions.ss, session)
}

func (s *StreamService) removeSession(session *internals.Session) {
	s.sessions.Lock()
	defer s.sessions.Unlock()
	for i, ss := range s.sessions.ss {
		if ss == session {
			s.sessions.ss = append(s.sessions.ss[:i], s.sessions.ss[i+1:]...)
			break
		}
	}
}

func (s *StreamService) BroadcastMsg(msg *message.QMessage) {
	s.sessions.Lock()
	defer s.sessions.Unlock()

	for _, ss := range s.sessions.ss {
		ss.Write(msg)
	}
}

func (s *StreamService) handleNewSession(brokerCtx context.Context, session *internals.Session,
	brokerStoppableErrCh chan BrokerStoppableErrMsg,
	sessionCloseableErrCh chan SessionCloseableErrMsg,
	sustainableErrCh chan SustainableErrMsg) {
	session.Open()
	defer session.Close()

	s.addSession(session)
	defer s.removeSession(session)

	sessionCtx, cancelSession := context.WithCancel(context.Background())
	defer cancelSession()

	readCh, readErrCh, err := session.ContinuousRead(sessionCtx)
	if err != nil {
		return
	}

	err, pl := s.newPipelineBase(sessionCtx, session, convertToInterfaceChan(readCh))

	writeErrCh, err := session.ContinuousWrite(sessionCtx,
		convertToQMessageChan(pl.Take(0, 0)))
	if err != nil {
		return
	}

	sessionErrChs := append(pl.ErrChannels, readErrCh, writeErrCh)
	errCh := pqerror.MergeErrors(sessionErrChs...)

	for {
		select {
		case <-brokerCtx.Done():
			return
		case <-sessionCtx.Done():
			return
		case err := <-errCh:
			distinguishError(err, brokerStoppableErrCh, sessionCloseableErrCh, sustainableErrCh, session, cancelSession)
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

func (s *StreamService) newPipelineBase(ctx context.Context, sess *internals.Session, inlet chan interface{}) (error, *pipeline.Pipeline) {
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

func distinguishError(err error,
	brokerStoppableErrCh chan BrokerStoppableErrMsg,
	sessionCloseableErrCh chan SessionCloseableErrMsg,
	sustainableErrCh chan SustainableErrMsg,
	session *internals.Session, cancelSession context.CancelFunc) {
	var (
		ok            = false
		broadcastable = false
		clientVisible = false
		pqErr         pqerror.PQError
	)

	pqErr, ok = err.(pqerror.PQError)
	if !ok {
		brokerStoppableErrCh <- BrokerStoppableErrMsg{
			Err:           pqerror.UnhandledError{ErrStr: err.Error()},
			ClientVisible: false,
			Broadcastable: false,
		}
		return
	}

	switch err.(type) {
	case pqerror.IsClientVisible:
		clientVisible = true
	case pqerror.IsBroadcastable:
		broadcastable = true
	default:
	}

	switch err.(type) {
	case pqerror.IsSessionCloseable:
		sessionCloseableErrCh <- SessionCloseableErrMsg{
			Err:           pqErr,
			CancelSession: cancelSession,
			Session:       session,
			ClientVisible: clientVisible,
			Broadcastable: broadcastable,
		}
	case pqerror.IsBrokerStoppable:
		brokerStoppableErrCh <- BrokerStoppableErrMsg{
			Err:           pqErr,
			ClientVisible: clientVisible,
			Broadcastable: broadcastable,
		}
	default:
		sustainableErrCh <- SustainableErrMsg{
			Err:           pqErr,
			Session:       session,
			ClientVisible: clientVisible,
			Broadcastable: broadcastable,
		}
	}
}
