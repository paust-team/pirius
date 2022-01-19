package rpc

import (
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
)

type FragmentRPCService interface {
	CreateFragment(request *shapleqproto.CreateFragmentRequest) *shapleqproto.CreateFragmentResponse
	DeleteFragment(request *shapleqproto.DeleteFragmentRequest) *shapleqproto.DeleteFragmentResponse
}

type fragmentRPCService struct {
	DB        *storage.QRocksDB
	zkqClient *zookeeper.ZKQClient
}

func NewFragmentRPCService(db *storage.QRocksDB, zkClient *zookeeper.ZKQClient) *fragmentRPCService {
	return &fragmentRPCService{db, zkClient}
}

func (s *fragmentRPCService) createFragment(topicName string) *shapleqproto.CreateFragmentResponse {
	fragmentValue := common.NewFragmentDataFromValues(0, 0)
	fragmentId := common.GenerateFragmentId()

	err := s.zkqClient.AddTopicFragment(topicName, fragmentId, fragmentValue)
	if err != nil {
		if _, ok := err.(*pqerror.ZKTargetAlreadyExistsError); ok {
			return s.createFragment(topicName) // recursive create for duplicated fragment id
		}
		return message.NewCreateTopicFragmentResponseMsg(0, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewCreateTopicFragmentResponseMsg(fragmentId, nil)
}

func (s *fragmentRPCService) CreateFragment(request *shapleqproto.CreateFragmentRequest) *shapleqproto.CreateFragmentResponse {
	fragments, err := s.zkqClient.GetTopicFragments(request.TopicName)
	if err != nil {
		return message.NewCreateTopicFragmentResponseMsg(0, &pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	if len(fragments) >= common.MaxFragmentCount {
		return message.NewCreateTopicFragmentResponseMsg(0, &pqerror.TopicFragmentOutOfCapacityError{Topic: request.TopicName})
	}

	return s.createFragment(request.TopicName)
}

func (s *fragmentRPCService) DeleteFragment(request *shapleqproto.DeleteFragmentRequest) *shapleqproto.DeleteFragmentResponse {

	if err := s.zkqClient.RemoveTopicFragment(request.TopicName, request.FragmentId); err != nil {
		return message.NewDeleteTopicFragmentResponseMsg(&pqerror.ZKOperateError{ErrStr: err.Error()})
	}

	return message.NewDeleteTopicFragmentResponseMsg(nil)
}
