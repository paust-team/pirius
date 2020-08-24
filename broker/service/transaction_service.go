package service

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/service/rpc"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/zookeeper"
	"runtime"
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
		for {
			select {
			case <-brokerCtx.Done():
				return
			case eventStream, ok := <-eventStreamCh:
				if ok {
					wg.Add(1)
					go s.handleEventStream(eventStream, errCh, &wg)
				}

			default:
			}
			runtime.Gosched()
		}
	}()

	return errCh
}

func (s *TransactionService) handleEventStream(eventStream internals.EventStream, sessionErrCh chan error, wg *sync.WaitGroup) {

	defer wg.Done()
	readCh := eventStream.ReadMsgCh
	writeCh := eventStream.WriteMsgCh

	defer close(writeCh)

	for {
		select {
		case msg, ok := <-readCh:
			if ok {
				var resMsg proto.Message

				if reqMsg, err := msg.UnpackAs(&shapleqproto.CreateTopicRequest{}); err == nil {
					resMsg = s.rpcService.CreateTopic(reqMsg.(*shapleqproto.CreateTopicRequest))

				} else if reqMsg, err := msg.UnpackAs(&shapleqproto.DeleteTopicRequest{}); err == nil {
					resMsg = s.rpcService.DeleteTopic(reqMsg.(*shapleqproto.DeleteTopicRequest))

				} else if reqMsg, err := msg.UnpackAs(&shapleqproto.ListTopicRequest{}); err == nil {
					resMsg = s.rpcService.ListTopic(reqMsg.(*shapleqproto.ListTopicRequest))

				} else if reqMsg, err := msg.UnpackAs(&shapleqproto.DescribeTopicRequest{}); err == nil {
					resMsg = s.rpcService.DescribeTopic(reqMsg.(*shapleqproto.DescribeTopicRequest))

				} else if reqMsg, err := msg.UnpackAs(&shapleqproto.Ping{}); err == nil {
					resMsg = s.rpcService.Heartbeat(reqMsg.(*shapleqproto.Ping))

				} else if reqMsg, err := msg.UnpackAs(&shapleqproto.DiscoverBrokerRequest{}); err == nil {
					resMsg = s.rpcService.DiscoverBroker(reqMsg.(*shapleqproto.DiscoverBrokerRequest))
				} else {
					sessionErrCh <- err
				}

				qMsg, err := message.NewQMessageFromMsg(message.TRANSACTION, resMsg)
				if err != nil {
					sessionErrCh <- internals.TransactionalError{
						PQError:       err.(pqerror.PQError),
						Session:       eventStream.Session,
						CancelSession: eventStream.CancelSession,
					}
				} else {
					writeCh <- qMsg
				}
			} else {
				return
			}
		default:

		}
		runtime.Gosched()
	}
}
