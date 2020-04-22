package rpc

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/network"
	pipeline "github.com/paust-team/paustq/broker/pipeline"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	uuid "github.com/satori/go.uuid"
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
	sess := network.NewSession(common.NewSocketContainer(stream)).WithTopic(s.Topic)

	sess.Open()
	defer sess.Close()

	pl := pipeline.NewPipeline()

	var receivePipe, dispatchPipe, connectPipe, fetchPipe, putPipe, sendPipe1, sendPipe2, sendPipe3 pipeline.Pipe

	receivePipe = &pipeline.ReceivePipe{}
	err := receivePipe.Build(sess)
	if err != nil {
		return errors.New("failed to build receive pipe")
	}
	receiveNode := pipeline.NewPipeNode("receive", &receivePipe)

	dispatchPipe = &pipeline.DispatchPipe{}
	err = dispatchPipe.Build()
	dispatchNode := pipeline.NewPipeNode("dispatch", &dispatchPipe)

	connectPipe = &pipeline.ConnectPipe{}
	err = connectPipe.Build(sess)
	if err != nil {
		return errors.New("failed to build connect pipe")
	}
	connectNode := pipeline.NewPipeNode("connect", &connectPipe)

	fetchPipe = &pipeline.FetchPipe{}
	err = fetchPipe.Build(sess, s.DB)
	if err != nil {
		return errors.New("failed to build fetch pipe")
	}
	fetchNode := pipeline.NewPipeNode("fetch", &fetchPipe)

	putPipe = &pipeline.PutPipe{}
	err = putPipe.Build(sess, s.DB)
	if err != nil {
		return errors.New("failed to build pu pipe")
	}
	putNode := pipeline.NewPipeNode("put", &putPipe)

	sendPipe1 = &pipeline.SendPipe{}
	sendPipe2 = &pipeline.SendPipe{}
	sendPipe3 = &pipeline.SendPipe{}
	err = sendPipe1.Build(sess)
	if err != nil {
		return errors.New("failed to build send pipe")
	}
	err = sendPipe2.Build(sess)
	if err != nil {
		return errors.New("failed to build send pipe")
	}
	err = sendPipe3.Build(sess)
	if err != nil {
		return errors.New("failed to build send pipe")
	}
	sendNode1 := pipeline.NewPipeNode("send", &sendPipe1)
	sendNode2 := pipeline.NewPipeNode("send", &sendPipe2)
	sendNode3 := pipeline.NewPipeNode("send", &sendPipe3)


	err = pl.Add(uuid.UUID{}, receiveNode, nil)
	if err != nil {
		return err
	}
	err = pl.Add(receiveNode.ID(), dispatchNode, nil)
	if err != nil {
		return err
	}

	isConnectRequest := func(data interface{}) (interface{}, bool) {
		msg, ok := data.(*message.QMessage)
		if !ok {
			return nil, false
		}
		pb := &paustqproto.ConnectRequest{}
		err := msg.UnpackTo(pb)
		if err != nil {
			return nil, false
		}
		return pb, true
	}
	err = pl.Add(dispatchNode.ID(), connectNode, isConnectRequest)
	if err != nil {
		return err
	}
	err = pl.Add(connectNode.ID(), sendNode1, nil)
	if err != nil {
		return err
	}

	isFetchRequest := func(data interface{}) (interface{}, bool) {
		msg, ok := data.(*message.QMessage)
		if !ok {
			return nil, false
		}
		pb := &paustqproto.FetchRequest{}
		err := msg.UnpackTo(pb)
		if err != nil {
			return nil, false
		}
		return pb, true
	}

	err = pl.Add(dispatchNode.ID(), fetchNode, isFetchRequest)
	if err != nil {
		return err
	}
	err = pl.Add(fetchNode.ID(), sendNode2, nil)
	if err != nil {
		return err
	}

	isPutRequest := func(data interface{}) (interface{}, bool) {
		msg, ok := data.(*message.QMessage)
		if !ok {
			return nil, false
		}
		pb := &paustqproto.PutRequest{}
		err := msg.UnpackTo(pb)
		if err != nil {
			return nil, false
		}
		return pb, true
	}

	err = pl.Add(dispatchNode.ID(), putNode, isPutRequest)
	if err != nil {
		return err
	}

	err = pl.Add(putNode.ID(), sendNode3, nil)
	if err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	errCh, err := pl.Ready(ctx)
	if err != nil {
		return err
	}

	pl.Flow()

	err = pipeline.WaitForPipeline(errCh...)
	if err != nil {
		return err
	}

	return nil
}