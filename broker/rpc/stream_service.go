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
	DB  		*storage.QRocksDB
	pipeLine	*pipeline.Pipeline
	Topic 		*internals.Topic
}

func NewStreamServiceServer(db *storage.QRocksDB, topic *internals.Topic) *StreamServiceServer {
	return &StreamServiceServer{DB: db, Topic: topic}
}

func (s *StreamServiceServer) Flow(stream paustqproto.StreamService_FlowServer) error {
	sess := network.NewSession().WithTopic(s.Topic)
	sock := common.NewSocketContainer(stream)
	sock.Open()
	defer sock.Close()

	// build pipeline
	var dispatcher, connector, fetcher, putter, zipper pipeline.Pipe
	var err error

	dispatcher = &pipeline.DispatchPipe{}
	err = dispatcher.Build(pipeline.IsConnectRequest, pipeline.IsFetchRequest, pipeline.IsPutRequest)
	if err != nil {
		return err
	}
	dispatchPipe := pipeline.NewPipe("dispatch", &dispatcher)

	connector = &pipeline.ConnectPipe{}
	err = connector.Build(sess)
	if err != nil {
		return err
	}
	connectPipe := pipeline.NewPipe("connect", &connector)

	fetcher = &pipeline.FetchPipe{}
	err = fetcher.Build(sess, s.DB)
	if err != nil {
		return err
	}
	fetchPipe := pipeline.NewPipe("fetch", &fetcher)

	putter = &pipeline.PutPipe{}
	err = putter.Build(sess, s.DB)
	if err != nil {
		return err
	}
	putPipe := pipeline.NewPipe("put", &putter)

	zipper = &pipeline.ZipPipe{}
	err = zipper.Build()
	if err != nil {
		return err
	}
	zipPipe := pipeline.NewPipe("zip", &zipper)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	inlet := make(chan interface{})
	defer close(inlet)
	pl := pipeline.NewPipeline(inlet)

	if err = pl.Add(ctx, dispatchPipe, inlet); err != nil {
		return err
	}
	if err = pl.Add(ctx, connectPipe, dispatchPipe.Outlets[0]); err != nil {
		return err
	}
	if err = pl.Add(ctx, fetchPipe, dispatchPipe.Outlets[1]); err != nil {
		return err
	}
	if err = pl.Add(ctx, putPipe, dispatchPipe.Outlets[2]); err != nil {
		return err
	}
	if err = pl.Add(ctx, zipPipe, connectPipe.Outlets[0], fetchPipe.Outlets[0], putPipe.Outlets[0]); err != nil {
		return err
	}

	var msg *message.QMessage
	go func() {
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
		for outMsg := range pl.Take(ctx, 0, 0) {
			err = sock.Write(outMsg.(*message.QMessage))
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