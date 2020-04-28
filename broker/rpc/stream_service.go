package rpc

import (
	"context"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/network"
	pipeline "github.com/paust-team/paustq/broker/pipeline"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
)

type StreamServiceServer struct {
	DB       *storage.QRocksDB
	Notifier *internals.Notifier
}

func NewStreamServiceServer(db *storage.QRocksDB, notifier *internals.Notifier) *StreamServiceServer {
	return &StreamServiceServer{DB: db, Notifier: notifier}
}

func (s *StreamServiceServer) Flow(stream paustqproto.StreamService_FlowServer) error {

	sess := network.NewSession()
	sock := common.NewSocketContainer(stream)
	sock.Open()
	defer sock.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	inlet := make(chan interface{})
	defer close(inlet)

	err, pl := s.NewPipelineBase(ctx, sess, inlet)
	if err != nil {
		return err
	}

	var msg *message.QMessage
	go func() {
		defer cancelFunc()
		for {
			if msg, err = sock.Read(); err != nil {
				return
			}
			if msg == nil {
				return
			}
			pl.Flow(ctx, 0, msg)
		}
	}()

	go func() {
		defer cancelFunc()
		for outMsg := range pl.Take(ctx, 0, 0) {
			err = sock.Write(outMsg.(*message.QMessage), 1024)
			if err != nil {
				return
			}
		}
	}()

	err = pl.Wait(ctx)
	if err != nil {
		return err
	}

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