package rpc

import (
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
	"strconv"
)

type TopicRPCService interface {
	CreateTopic(*shapleqproto.CreateTopicRequest) *shapleqproto.CreateTopicResponse
	DeleteTopic(*shapleqproto.DeleteTopicRequest) *shapleqproto.DeleteTopicResponse
	ListTopic(*shapleqproto.ListTopicRequest) *shapleqproto.ListTopicResponse
	DescribeTopic(*shapleqproto.DescribeTopicRequest) *shapleqproto.DescribeTopicResponse
}

type topicRPCService struct {
	DB        *storage.QRocksDB
	zkqClient *zookeeper.ZKQClient
}

func NewTopicRPCService(db *storage.QRocksDB, zkqClient *zookeeper.ZKQClient) *topicRPCService {
	return &topicRPCService{db, zkqClient}
}

func (s topicRPCService) CreateTopic(request *shapleqproto.CreateTopicRequest) *shapleqproto.CreateTopicResponse {

	topicValue := common.NewTopicDataFromValues(request.TopicDescription, 0, 0, 0)
	err := s.zkqClient.AddTopic(request.TopicName, topicValue)
	if err != nil {
		return message.NewCreateTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewCreateTopicResponseMsg(nil)
}

func (s topicRPCService) DeleteTopic(request *shapleqproto.DeleteTopicRequest) *shapleqproto.DeleteTopicResponse {

	if err := s.zkqClient.RemoveTopic(request.TopicName); err != nil {
		return message.NewDeleteTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDeleteTopicResponseMsg(nil)
}

func (s topicRPCService) ListTopic(_ *shapleqproto.ListTopicRequest) *shapleqproto.ListTopicResponse {

	topics, err := s.zkqClient.GetTopics()
	if err != nil {
		return message.NewListTopicResponseMsg(nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewListTopicResponseMsg(topics, nil)
}

func (s topicRPCService) DescribeTopic(request *shapleqproto.DescribeTopicRequest) *shapleqproto.DescribeTopicResponse {

	topicValue, err := s.zkqClient.GetTopicData(request.TopicName)

	if err != nil {
		return message.NewDescribeTopicResponseMsg("", "", 0, nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	fragments, err := s.zkqClient.GetTopicFragments(request.TopicName)
	if err != nil {
		return message.NewDescribeTopicResponseMsg("", "", 0, nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}
	var fragmentIds []uint32

	for _, fragment := range fragments {
		fragmentId, err := strconv.ParseUint(fragment, 10, 32)
		if err != nil {
			continue
		}
		fragmentIds = append(fragmentIds, uint32(fragmentId))
	}

	return message.NewDescribeTopicResponseMsg(request.TopicName, topicValue.Description(), topicValue.ReplicationFactor(), fragmentIds, nil)
}
