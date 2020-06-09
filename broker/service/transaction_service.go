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

	msgHandler.RegisterMsgHandle(&paustqproto.CreateTopicRequest{}, func(msg proto.Message, args ...interface{}) {
		session, ok := args[0].(*internals.Session)
		if !ok {
			handleErrCh <- pqerror.UnhandledError{ErrStr: "session argument required"}
			return
		}
		res := service.apiService.CreateTopic(msg.(*paustqproto.CreateTopicRequest))
		qMsg, err := message.NewQMessageFromMsg(res)

		if err != nil {
			handleErrCh <- err
			return
		}
		if err := session.Write(qMsg); err != nil {
			handleErrCh <- err
		}
	})

	msgHandler.RegisterMsgHandle(&paustqproto.DeleteTopicRequest{}, func(msg proto.Message, args ...interface{}) {
		session, ok := args[0].(*internals.Session)
		if !ok {
			handleErrCh <- pqerror.UnhandledError{ErrStr: "session argument required"}
			return
		}
		res := service.apiService.DeleteTopic(msg.(*paustqproto.DeleteTopicRequest))
		qMsg, err := message.NewQMessageFromMsg(res)

		if err != nil {
			handleErrCh <- err
			return
		}
		if err := session.Write(qMsg); err != nil {
			handleErrCh <- err
		}
	})

	return msgHandler, handleErrCh
}

