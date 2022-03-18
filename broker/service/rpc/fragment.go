package rpc

import (
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/common"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/coordinator-helper/helper"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
)

type FragmentRPCService interface {
	CreateFragment(request *shapleqproto.CreateFragmentRequest) *shapleqproto.CreateFragmentResponse
	DeleteFragment(request *shapleqproto.DeleteFragmentRequest) *shapleqproto.DeleteFragmentResponse
	DescribeFragment(request *shapleqproto.DescribeFragmentRequest) *shapleqproto.DescribeFragmentResponse
}

type fragmentRPCService struct {
	DB            *storage.QRocksDB
	coordiWrapper *coordinator_helper.CoordinatorWrapper
}

func NewFragmentRPCService(db *storage.QRocksDB, coordiWrapper *coordinator_helper.CoordinatorWrapper) *fragmentRPCService {
	return &fragmentRPCService{db, coordiWrapper}
}

func (s *fragmentRPCService) createFragment(topicName string) *shapleqproto.CreateFragmentResponse {
	fragmentFrame := helper.NewFrameForFragmentFromValues(0, 0)
	fragmentId := common.GenerateFragmentId()

	err := s.coordiWrapper.AddTopicFragment(topicName, fragmentId, fragmentFrame)
	if err != nil {
		if _, ok := err.(*pqerror.ZKTargetAlreadyExistsError); ok {
			return s.createFragment(topicName) // recursive create for duplicated fragment id
		}
		return message.NewCreateTopicFragmentResponseMsg(0, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewCreateTopicFragmentResponseMsg(fragmentId, nil)
}

func (s *fragmentRPCService) CreateFragment(request *shapleqproto.CreateFragmentRequest) *shapleqproto.CreateFragmentResponse {
	fragments, err := s.coordiWrapper.GetTopicFragments(request.TopicName)
	if err != nil {
		return message.NewCreateTopicFragmentResponseMsg(0, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	if len(fragments) >= common.MaxFragmentCount {
		return message.NewCreateTopicFragmentResponseMsg(0, &pqerror.TopicFragmentOutOfCapacityError{Topic: request.TopicName})
	}

	return s.createFragment(request.TopicName)
}

func (s *fragmentRPCService) DeleteFragment(request *shapleqproto.DeleteFragmentRequest) *shapleqproto.DeleteFragmentResponse {

	if err := s.coordiWrapper.RemoveTopicFragment(request.TopicName, request.FragmentId); err != nil {
		return message.NewDeleteTopicFragmentResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDeleteTopicFragmentResponseMsg(nil)
}

func (s *fragmentRPCService) DescribeFragment(request *shapleqproto.DescribeFragmentRequest) *shapleqproto.DescribeFragmentResponse {
	fragmentData, err := s.coordiWrapper.GetTopicFragmentFrame(request.TopicName, request.FragmentId)
	if err != nil {
		return message.NewDescribeTopicFragmentResponseMsg(0, 0, nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	brokerAddresses, err := s.coordiWrapper.GetBrokersOfTopic(request.TopicName, request.FragmentId)
	if err != nil {
		return message.NewDescribeTopicFragmentResponseMsg(0, 0, nil, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDescribeTopicFragmentResponseMsg(request.FragmentId, fragmentData.LastOffset(), brokerAddresses, nil)
}
