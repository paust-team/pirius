package service

import (
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/service/rpc"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"sync"
)

type RPCService struct {
	rpc.TopicRPCService
	rpc.PartitionRPCService
	rpc.ConfigRPCService
	rpc.GroupRPCService
	rpc.ConnectionRPCService
}

type TransactionService struct {
	rpcService *RPCService
}

func NewTransactionService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *TransactionService {
	return &TransactionService{&RPCService{
		rpc.NewTopicRPCService(db, zkClient),
		rpc.NewPartitionRPCService(db, zkClient),
		rpc.NewConfigRPCService(),
		rpc.NewGroupRPCService(),
		rpc.NewConnectionRPCService(zkClient)},
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
		select {
		case <-brokerCtx.Done():
			return
		case eventStream := <-eventStreamCh:
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case msg := <-eventStream.MsgCh:

						if msg == nil {
							return
						}
						err := s.handleMsg(msg, eventStream.Session)
						errCh <- err
					}
				}
			}()
		}
	}()

	return errCh
}

func (s *TransactionService) handleMsg(msg *message.QMessage, session *internals.Session) error {

	var resMsg proto.Message

	if reqMsg, err := msg.UnpackAs(&paustqproto.CreateTopicRequest{}); err == nil {
		resMsg = s.rpcService.CreateTopic(reqMsg.(*paustqproto.CreateTopicRequest))

	} else if reqMsg, err := msg.UnpackAs(&paustqproto.DeleteTopicRequest{}); err == nil {
		resMsg = s.rpcService.DeleteTopic(reqMsg.(*paustqproto.DeleteTopicRequest))

	} else if reqMsg, err := msg.UnpackAs(&paustqproto.ListTopicRequest{}); err == nil {
		resMsg = s.rpcService.ListTopic(reqMsg.(*paustqproto.ListTopicRequest))

	} else if reqMsg, err := msg.UnpackAs(&paustqproto.DescribeTopicRequest{}); err == nil {
		resMsg = s.rpcService.DescribeTopic(reqMsg.(*paustqproto.DescribeTopicRequest))

	} else if reqMsg, err := msg.UnpackAs(&paustqproto.Ping{}); err == nil {
		resMsg = s.rpcService.Heartbeat(reqMsg.(*paustqproto.Ping))

	} else if reqMsg, err := msg.UnpackAs(&paustqproto.DiscoverBrokerRequest{}); err == nil {
		resMsg = s.rpcService.DiscoverBroker(reqMsg.(*paustqproto.DiscoverBrokerRequest))
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
