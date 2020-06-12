package rpc

import (
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/pqerror"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"math/rand"
	"time"
)

type ConnectionRPCService interface {
	DiscoverBroker(*paustqproto.DiscoverBrokerRequest) *paustqproto.DiscoverBrokerResponse
	Heartbeat(*paustqproto.Ping) *paustqproto.Pong
}

type connectionRPCService struct {
	zkClient *zookeeper.ZKClient
}

func NewConnectionRPCService(zkClient *zookeeper.ZKClient) *connectionRPCService {
	return &connectionRPCService{zkClient}
}

func (s *connectionRPCService) DiscoverBroker(request *paustqproto.DiscoverBrokerRequest) *paustqproto.DiscoverBrokerResponse {

	topicBrokerAddrs, err := s.zkClient.GetTopicBrokers(request.TopicName)
	if err != nil {
		return message.NewDiscoverBrokerResponseMsg("", &pqerror.ZKOperateError{ErrStr: err.Error()})

	} else if len(topicBrokerAddrs) > 0 {
		return message.NewDiscoverBrokerResponseMsg(topicBrokerAddrs[0], nil)

	} else {
		if request.SessionType == paustqproto.SessionType_PUBLISHER {
			brokerAddrs, err := s.zkClient.GetBrokers()
			if err == nil && len(brokerAddrs) != 0 {
				brokerAddr := brokerAddrs[rand.Intn(len(brokerAddrs))] // pick random broker
				return message.NewDiscoverBrokerResponseMsg(brokerAddr, nil)
			} else {
				return message.NewDiscoverBrokerResponseMsg("", &pqerror.UnhandledError{ErrStr: "no brokers"})
			}
		}
		return message.NewDiscoverBrokerResponseMsg("", &pqerror.UnhandledError{ErrStr: "no brokers"})
	}
}

func (s *connectionRPCService) Heartbeat(request *paustqproto.Ping) *paustqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}
