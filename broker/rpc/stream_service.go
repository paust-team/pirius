package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/network"
	pipeline "github.com/paust-team/paustq/broker/pipeline"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"sync"
	"sync/atomic"
)

type StreamServiceServer struct {
	DB          *storage.QRocksDB
	Notifier    *internals.Notifier
	zKClient    *zookeeper.ZKClient
	host        string
	broadcaster *internals.Broadcaster
	brokerErrCh chan error
}

func NewStreamServiceServer(db *storage.QRocksDB, notifier *internals.Notifier, zkClient *zookeeper.ZKClient,
	host string, broadcaster *internals.Broadcaster, brokerErrCh chan error) *StreamServiceServer {
	return &StreamServiceServer{
		DB:          db,
		Notifier:    notifier,
		zKClient:    zkClient,
		host:        host,
		broadcaster: broadcaster,
		brokerErrCh: brokerErrCh,
	}
}

func (s *StreamServiceServer) Flow(stream paustqproto.StreamService_FlowServer) error {
	sess := network.NewSession()
	sock := common.NewSocketContainer(stream)

	sock.Open()
	defer HandleConnectionClose(sess, sock)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	inlet := make(chan interface{})
	err, pl := s.NewPipelineBase(ctx, sess, inlet)
	defer func() {
		// close inlet after all of the Flow functions are done
		pl.FlowingGroup.Wait()
		close(inlet)
	}()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	msgCh, readErrCh := sock.ContinuousRead()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-msgCh:
				if msg != nil {
					pl.Flow(ctx, 0, msg)
				}
			}
		}
	}()

	writeCh := make(chan *message.QMessage)
	defer close(writeCh)

	s.broadcaster.AddChannel(writeCh)
	defer s.broadcaster.RemoveChannel(writeCh)

	writeErrCh := sock.ContinuousWrite(writeCh)
	msgStream := pl.Take(ctx, 0, 0)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-msgStream:
				writeCh <- msg.(*message.QMessage)
			}
		}
	}()

	sessionErrChs := append(pl.ErrChannels, readErrCh, writeErrCh)
	HandleErrors(ctx, cancelFunc, writeCh, sessionErrChs, s.brokerErrCh, s.broadcaster, pl.PipeGroup)

	wg.Wait()
	return nil
}

func (s *StreamServiceServer) NewPipelineBase(ctx context.Context, sess *network.Session, inlet chan interface{}) (error, *pipeline.Pipeline) {
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

	if err = pl.Add(ctx, dispatchPipe, inlet); err != nil {
		return err, nil
	}
	if err = pl.Add(ctx, connectPipe, dispatchPipe.Outlets[0]); err != nil {
		return err, nil
	}
	if err = pl.Add(ctx, fetchPipe, dispatchPipe.Outlets[1]); err != nil {
		return err, nil
	}
	if err = pl.Add(ctx, putPipe, dispatchPipe.Outlets[2]); err != nil {
		return err, nil
	}
	if err = pl.Add(ctx, zipPipe, connectPipe.Outlets[0], fetchPipe.Outlets[0], putPipe.Outlets[0]); err != nil {
		return err, nil
	}

	return nil, pl
}

func HandleConnectionClose(sess *network.Session, sock *common.StreamSocketContainer) {
	switch sess.Type() {
	case paustqproto.SessionType_PUBLISHER:
		if atomic.LoadInt64(&sess.Topic().NumPubs) > 0 {
			atomic.AddInt64(&sess.Topic().NumPubs, -1)
		}
	case paustqproto.SessionType_SUBSCRIBER:
		if atomic.LoadInt64(&sess.Topic().NumSubs) > 0 {
			atomic.AddInt64(&sess.Topic().NumSubs, -1)
		}
	}

	sock.Close()
	sess.SetState(network.NONE)
}

func HandleErrors(sessionCtx context.Context, cancelFunc context.CancelFunc,
	writeChan chan *message.QMessage, errChannels []<-chan error, brokerErrCh chan error,
	broadcaster *internals.Broadcaster, pipeGroup *sync.WaitGroup) {
	errCh := pqerror.MergeErrors(errChannels...)

	go func() {
		for {
			select {
			case <-sessionCtx.Done():
				return
			case err := <-errCh:
				if err != nil {
					switch err.(type) {
					case pqerror.IsClientVisible:
						pqErr, ok := err.(pqerror.PQError)
						if !ok {
							brokerErrCh <- pqerror.UnhandledError{ErrStr: err.Error()}
							return
						}
						writeChan <- message.NewErrorAckMsg(pqErr.Code(), pqErr.Error())
					case pqerror.IsBroadcastable:
						pqErr, ok := err.(pqerror.PQError)
						if !ok {
							brokerErrCh <- pqerror.UnhandledError{ErrStr: err.Error()}
							return
						}

						broadcaster.Broadcast(message.NewErrorAckMsg(pqErr.Code(), pqErr.Error()))
					default:
					}

					switch err.(type) {
					case pqerror.IsSessionCloseable:
						cancelFunc()
						// guarantee all pipes are closed after context done
						pipeGroup.Wait()
						return
					case pqerror.IsBrokerStoppable:
						brokerErrCh <- err
						return
					default:
					}
				}
			}
		}
	}()
}
