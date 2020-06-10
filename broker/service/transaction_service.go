package service

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/service/rpc"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
)

type RPCService struct {
	rpc.TopicRPCService
	rpc.PartitionRPCService
	rpc.ConfigRPCService
	rpc.GroupRPCService
	rpc.HeartBeatRPCService
}

type TransactionService struct {
	rpcService *RPCService
}

type SessionMessageHandler struct {
	*message.BaseHandler
}

func (h *SessionMessageHandler) RegisterMsgHandle(msg proto.Message, f func(msg proto.Message, session *internals.Session)) {
	wrappedFn := func(msg proto.Message, args ...interface{}) {
		if len(args) == 1 {
			sess, ok := args[0].(*internals.Session)
			if ok {
				f(msg, sess)
			}
		}
	}
	h.BaseHandler.RegisterMsgHandle(msg, wrappedFn)
}

func NewTransactionService(db *storage.QRocksDB, zkClient *zookeeper.ZKClient) *TransactionService {
	return &TransactionService{&RPCService{
			rpc.NewTopicRPCService(db, zkClient),
			rpc.NewPartitionRPCService(db, zkClient),
			rpc.NewConfigRPCService(),
			rpc.NewGroupRPCService(),
			rpc.NewHeartbeatService(),
		},
	}
}

func (s *TransactionService) HandleEventStreams(brokerCtx context.Context, eventStreamCh <- chan internals.EventStream) <- chan error {
	errCh := make(chan error)
	msgHandler, msgHandlerErrCh := s.registerMessageHandles()

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

func (s *TransactionService) registerMessageHandles() (message.Handler, <- chan error) {

	msgHandler := &SessionMessageHandler{}
	handleErrCh := make(chan error)

	handleSessionMsg := func(responseMsg proto.Message, session *internals.Session) {

		qMsg, err := message.NewQMessageFromMsg(responseMsg)

		if err != nil {
			handleErrCh <- err
			return
		}
		if err := session.Write(qMsg); err != nil {
			handleErrCh <- err
		}
	}

	msgHandler.RegisterMsgHandle(&paustqproto.CreateTopicRequest{}, func(msg proto.Message, session *internals.Session) {
		handleSessionMsg(s.rpcService.CreateTopic(msg.(*paustqproto.CreateTopicRequest)), session)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.DeleteTopicRequest{}, func(msg proto.Message, session *internals.Session) {
		handleSessionMsg(s.rpcService.DeleteTopic(msg.(*paustqproto.DeleteTopicRequest)), session)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.ListTopicRequest{}, func(msg proto.Message, session *internals.Session) {
		handleSessionMsg(s.rpcService.ListTopic(msg.(*paustqproto.ListTopicRequest)), session)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.DescribeTopicRequest{}, func(msg proto.Message, session *internals.Session) {
		handleSessionMsg(s.rpcService.DescribeTopic(msg.(*paustqproto.DescribeTopicRequest)), session)
	})

	msgHandler.RegisterMsgHandle(&paustqproto.Ping{}, func(msg proto.Message, session *internals.Session) {
		handleSessionMsg(s.rpcService.Heartbeat(msg.(*paustqproto.Ping)), session)
	})

	return msgHandler, handleErrCh
}

