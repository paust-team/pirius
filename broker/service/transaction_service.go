package service

import (
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/service/rpc"
	"github.com/paust-team/shapleq/broker/storage"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/message"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"runtime"
	"sync"
)

type RPCService struct {
	rpc.TopicRPCService
	rpc.FragmentRPCService
	rpc.ConfigRPCService
	rpc.ConnectionRPCService
}

type TransactionService struct {
	rpcService *RPCService
}

func NewTransactionService(db *storage.QRocksDB, coordiWrapper *coordinator_helper.CoordinatorWrapper) *TransactionService {
	return &TransactionService{&RPCService{
		rpc.NewTopicRPCService(db, coordiWrapper),
		rpc.NewFragmentRPCService(db, coordiWrapper),
		rpc.NewConfigRPCService(),
		rpc.NewConnectionRPCService(coordiWrapper)},
	}
}

func (s *TransactionService) HandleEventStreams(brokerCtx context.Context, eventStreamCh <-chan internals.EventStream) <-chan error {
	errCh := make(chan error)
	var wg sync.WaitGroup
	go func() {
		defer func() {
			wg.Wait()
			close(errCh)
		}()
		for {
			select {
			case <-brokerCtx.Done():
				return
			case eventStream, ok := <-eventStreamCh:
				if ok {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							select {
							case msg := <-eventStream.MsgCh:
								if msg == nil {
									return
								}
								if err := s.handleMsg(msg, eventStream.Session); err != nil {
									errCh <- err
								}
							}
							runtime.Gosched()
						}
					}()
				}
			}
		}
	}()

	return errCh
}

func (s *TransactionService) handleMsg(msg *message.QMessage, session *internals.Session) error {

	var resMsg proto.Message

	if reqMsg, err := msg.UnpackTo(&shapleqproto.CreateTopicRequest{}); err == nil {
		resMsg = s.rpcService.CreateTopic(reqMsg.(*shapleqproto.CreateTopicRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.DeleteTopicRequest{}); err == nil {
		resMsg = s.rpcService.DeleteTopic(reqMsg.(*shapleqproto.DeleteTopicRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.ListTopicRequest{}); err == nil {
		resMsg = s.rpcService.ListTopic(reqMsg.(*shapleqproto.ListTopicRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.DescribeTopicRequest{}); err == nil {
		resMsg = s.rpcService.DescribeTopic(reqMsg.(*shapleqproto.DescribeTopicRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.CreateFragmentRequest{}); err == nil {
		resMsg = s.rpcService.CreateFragment(reqMsg.(*shapleqproto.CreateFragmentRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.DeleteFragmentRequest{}); err == nil {
		resMsg = s.rpcService.DeleteFragment(reqMsg.(*shapleqproto.DeleteFragmentRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.DescribeFragmentRequest{}); err == nil {
		resMsg = s.rpcService.DescribeFragment(reqMsg.(*shapleqproto.DescribeFragmentRequest))

	} else if reqMsg, err := msg.UnpackTo(&shapleqproto.Ping{}); err == nil {
		resMsg = s.rpcService.Heartbeat(reqMsg.(*shapleqproto.Ping))

	} else {
		return errors.New("invalid message to handle")
	}

	qMsg, err := message.NewQMessageFromMsg(message.TRANSACTION, resMsg)
	if err != nil {
		return err
	}
	if err := session.Write(qMsg); err != nil {
		return err
	}
	return nil
}
