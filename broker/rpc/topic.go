package rpc

import (
	"context"
	"github.com/paust-team/pirius/bootstrapping/topic"
	"github.com/paust-team/pirius/proto/pb"
	"github.com/paust-team/pirius/qerror"
)

type TopicService struct {
	pb.TopicServer
	coordClient topic.CoordClientTopicWrapper
}

func NewTopicService(topicCoordClient topic.CoordClientTopicWrapper) TopicService {
	return TopicService{coordClient: topicCoordClient}
}

func (s TopicService) CreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.Empty, error) {
	if len(request.GetName()) == 0 {
		return nil, qerror.ValidationError{Value: request.GetName(), HintMsg: "name should not be blank"}
	}
	topicFrame := topic.NewTopicFrame(request.GetDescription(), topic.Option(int(request.GetOptions())))
	if err := s.coordClient.CreateTopic(request.GetName(), topicFrame); err != nil {
		return nil, err
	}
	return &pb.Empty{Magic: 1}, nil
}

func (s TopicService) GetTopic(ctx context.Context, request *pb.TopicRequestWithName) (*pb.TopicInfo, error) {
	if frame, err := s.coordClient.GetTopic(request.GetName()); err != nil {
		return nil, err
	} else {
		options := uint32(frame.Options())
		return &pb.TopicInfo{
			Name:        request.GetName(),
			Description: frame.Description(),
			Options:     &options,
		}, nil
	}
}

func (s TopicService) DeleteTopic(ctx context.Context, request *pb.TopicRequestWithName) (*pb.Empty, error) {
	if len(request.GetName()) == 0 {
		return nil, qerror.ValidationError{Value: request.GetName(), HintMsg: "name should not be blank"}
	}
	if err := s.coordClient.DeleteTopic(request.GetName()); err != nil {
		return nil, err
	}
	return &pb.Empty{Magic: 1}, nil
}

func (s TopicService) ListTopics(context.Context, *pb.Empty) (*pb.NameList, error) {
	if topics, err := s.coordClient.GetTopics(); err != nil {
		return nil, err
	} else {
		return &pb.NameList{
			Names: topics,
		}, nil
	}
}
