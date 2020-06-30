package rpc

import (
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"github.com/paust-team/shapleq/zookeeper"
	"math/rand"
	"time"
)

type ConnectionRPCService interface {
	DiscoverBroker(*shapleqproto.DiscoverBrokerRequest) *shapleqproto.DiscoverBrokerResponse
	Heartbeat(*shapleqproto.Ping) *shapleqproto.Pong
}

type connectionRPCService struct {
	zkClient *zookeeper.ZKClient
}

func NewConnectionRPCService(zkClient *zookeeper.ZKClient) *connectionRPCService {
	return &connectionRPCService{zkClient}
}

func (s *connectionRPCService) DiscoverBroker(request *shapleqproto.DiscoverBrokerRequest) *shapleqproto.DiscoverBrokerResponse {

	topicBrokerAddrs, err := s.zkClient.GetTopicBrokers(request.TopicName)
	if err != nil {
		return message.NewDiscoverBrokerResponseMsg("", &pqerror.ZKOperateError{ErrStr: err.Error()})

	} else if len(topicBrokerAddrs) > 0 {
		return message.NewDiscoverBrokerResponseMsg(topicBrokerAddrs[0], nil)

	} else {
		if request.SessionType == shapleqproto.SessionType_PUBLISHER {
			brokerAddrs, err := s.zkClient.GetBrokers()
			if err == nil && len(brokerAddrs) != 0 {
				brokerAddr := brokerAddrs[rand.Intn(len(brokerAddrs))] // pick random broker
				return message.NewDiscoverBrokerResponseMsg(brokerAddr, nil)
			} else {
				return message.NewDiscoverBrokerResponseMsg("", err.(pqerror.PQError))
			}
		}
		return message.NewDiscoverBrokerResponseMsg("", &pqerror.TopicBrokersNotExistError{})
	}
}

func (s *connectionRPCService) Heartbeat(request *shapleqproto.Ping) *shapleqproto.Pong {
	serverTime := time.Now().Nanosecond()
	return message.NewPongMsg(request.Echo, 1, uint64(serverTime))
}
