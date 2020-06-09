package service

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/service/rpc"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
)

type TransactionService struct {
	apiService *rpc.APIServiceServer
}

func NewTransactionService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *TransactionService {
	return &TransactionService{rpc.NewAPIServiceServer(db, zkClient)}
}

func (service *TransactionService) HandleEventStreams(brokerCtx context.Context, eventStreamCh <- chan internals.EventStream) <- chan error {
	errCh := make(chan error)
	msgHandler, msgHandlerErrCh := service.registerMessageHandles()

	go func() {
		select {
		case <-brokerCtx.Done():
			return
		case eventStream := <- eventStreamCh:
			go func() {
				for {
					select {
					case err := <- msgHandlerErrCh:
						errCh <- err
					case msg, ok := <- eventStream.MsgCh:
						if !ok {
							return
						}
						err := msgHandler.Handle(msg, eventStream.Session)
						errCh <- err
					}
				}
			}()
		}
	}()

	return errCh
}

func (service *TransactionService) registerMessageHandles() (*message.Handler, <- chan error) {

	msgHandler := &message.Handler{}
	handleErrCh := make(chan error)

	handleSessionMsg := func(responseMsg proto.Message, args ...interface{}) {
		if len(args) == 0 {
			handleErrCh <- pqerror.UnhandledError{ErrStr: "session argument required"}
			return
		}

		session, ok := args[0].(*internals.Session)
		if !ok {
			handleErrCh <- pqerror.UnhandledError{ErrStr: "session argument required"}
			return
		}

		qMsg, err := message.NewQMessageFromMsg(responseMsg)

		if err != nil {
			handleErrCh <- err
			return
		}
		if err := session.Write(qMsg); err != nil {
			handleErrCh <- err
		}
	}

	msgHandler.RegisterMsgHandle(&paustqproto.CreateTopicRequest{}, func(msg proto.Message, args ...interface{}) {
		handleSessionMsg(service.apiService.CreateTopic(msg.(*paustqproto.CreateTopicRequest)), args...)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.DeleteTopicRequest{}, func(msg proto.Message, args ...interface{}) {
		handleSessionMsg(service.apiService.DeleteTopic(msg.(*paustqproto.DeleteTopicRequest)), args...)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.ListTopicRequest{}, func(msg proto.Message, args ...interface{}) {
		handleSessionMsg(service.apiService.ListTopic(msg.(*paustqproto.ListTopicRequest)), args...)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.DescribeTopicRequest{}, func(msg proto.Message, args ...interface{}) {
		handleSessionMsg(service.apiService.DescribeTopic(msg.(*paustqproto.DescribeTopicRequest)), args...)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.Ping{}, func(msg proto.Message, args ...interface{}) {
		handleSessionMsg(service.apiService.Heartbeat(msg.(*paustqproto.Ping)), args...)
	})

	return msgHandler, handleErrCh
}

