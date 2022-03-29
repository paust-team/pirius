package rpc

import (
	"github.com/paust-team/shapleq/broker/storage"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/coordinator-helper/helper"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"strconv"
)

type TopicRPCService interface {
	CreateTopic(*shapleqproto.CreateTopicRequest) *shapleqproto.CreateTopicResponse
	DeleteTopic(*shapleqproto.DeleteTopicRequest) *shapleqproto.DeleteTopicResponse
	ListTopic(*shapleqproto.ListTopicRequest) *shapleqproto.ListTopicResponse
	DescribeTopic(*shapleqproto.DescribeTopicRequest) *shapleqproto.DescribeTopicResponse
}

type topicRPCService struct {
	DB            *storage.QRocksDB
	coordiWrapper *coordinator_helper.CoordinatorWrapper
}

func NewTopicRPCService(db *storage.QRocksDB, coordiWrapper *coordinator_helper.CoordinatorWrapper) *topicRPCService {
	return &topicRPCService{db, coordiWrapper}
}

func (s topicRPCService) CreateTopic(request *shapleqproto.CreateTopicRequest) *shapleqproto.CreateTopicResponse {

	topicFrame := helper.NewFrameForTopicFromValues(request.TopicDescription, 0, 0, 0)
	err := s.coordiWrapper.AddTopicFrame(request.TopicName, topicFrame)
	if err != nil {
		return message.NewCreateTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewCreateTopicResponseMsg(nil)
}

func (s topicRPCService) DeleteTopic(request *shapleqproto.DeleteTopicRequest) *shapleqproto.DeleteTopicResponse {

	if err := s.coordiWrapper.RemoveTopicFrame(request.TopicName); err != nil {
		return message.NewDeleteTopicResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDeleteTopicResponseMsg(nil)
}

func (s topicRPCService) ListTopic(_ *shapleqproto.ListTopicRequest) *shapleqproto.ListTopicResponse {

	topics, err := s.coordiWrapper.GetTopicFrames()
	if err != nil {
		return message.NewListTopicResponseMsg(nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewListTopicResponseMsg(topics, nil)
}

func (s topicRPCService) DescribeTopic(request *shapleqproto.DescribeTopicRequest) *shapleqproto.DescribeTopicResponse {

	topicValue, err := s.coordiWrapper.GetTopicFrame(request.TopicName)

	if err != nil {
		return message.NewDescribeTopicResponseMsg("", "", 0, nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	fragments, err := s.coordiWrapper.GetTopicFragments(request.TopicName)
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
