package agent

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/pirius/agent/config"
	"github.com/paust-team/pirius/agent/pubsub"
	"github.com/paust-team/pirius/agent/storage"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/proto/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

type RetrievablePubSubAgent struct {
	instance
	subscriber pubsub.RetrievableSubscriber
	publisher  pubsub.RetrievablePublisher
}

func NewRetrievablePubSubAgent(config config.AgentConfig) *RetrievablePubSubAgent {
	return &RetrievablePubSubAgent{
		instance: instance{
			config: config,
		},
	}
}

func (s *RetrievablePubSubAgent) StartWithServer() error {
	if err := s.instance.Start(); err != nil {
		return err
	}

	s.subscriber = pubsub.NewRetrievableSubscriber(s.meta.SubscriberID, s.bootstrapper, s.meta.SubscribedOffsets)
	agentAddress := fmt.Sprintf("%s:%d", s.config.Host(), s.config.Port())
	s.publisher = pubsub.NewRetrievablePublisher(s.meta.PublisherID, agentAddress, s.db, s.bootstrapper, s.meta.PublishedOffsets, s.meta.LastFetchedOffset)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterRetrievablePubSubServer(grpcServer, &s.publisher)

	lis, err := net.Listen("tcp", agentAddress)
	logger.Debug("grpc server listening", zap.String("publisher-id", s.meta.SubscriberID), zap.String("address", agentAddress))
	if err != nil {
		s.running = false
		return err
	}

	s.grpcServer = grpcServer
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		//start retention scheduler
		retentionScheduler := storage.NewRetentionScheduler(s.db, s.config.RetentionCheckInterval())
		retentionScheduler.Run(ctx)

		if err = grpcServer.Serve(lis); err != nil {
			logger.Error(err.Error(), zap.String("publisher-id", s.meta.PublisherID))
		} else {
			logger.Info("grpc server stopped", zap.String("publisher-id", s.meta.PublisherID))
		}
		s.running = false
	}()
	return nil
}

func (s *RetrievablePubSubAgent) Start() error {
	if err := s.instance.Start(); err != nil {
		return err
	}

	s.subscriber = pubsub.NewRetrievableSubscriber(s.meta.SubscriberID, s.bootstrapper, s.meta.SubscribedOffsets)
	agentAddress := fmt.Sprintf("%s:%d", s.config.Host(), s.config.Port())
	s.publisher = pubsub.NewRetrievablePublisher(s.meta.PublisherID, agentAddress, s.db, s.bootstrapper, s.meta.PublishedOffsets, s.meta.LastFetchedOffset)

	return nil
}

func (s *RetrievablePubSubAgent) StartRetrievablePublish(ctx context.Context, topicName string, sendChan chan pubsub.TopicData) (chan []pubsub.TopicDataResult, error) {
	if !s.running || s.grpcServer == nil {
		return nil, errors.New("not running state")
	}

	retentionPeriod := uint64(s.config.RetentionPeriod() * 60 * 60 * 24)
	ctx, cancel := context.WithCancel(ctx)

	retrieveCh, errCh, err := s.publisher.StartTopicPublication(ctx, topicName, retentionPeriod, sendChan)
	if err != nil {
		cancel()
		return nil, err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.publisher.Wait()
		defer cancel()
		for {
			select {
			case err = <-errCh:
				if err != nil {
					logger.Error(err.Error())
				}
				return
			case <-s.shouldQuit:
				logger.Info("stop publish from agent stopped")
				return
			}
		}
	}()

	return retrieveCh, nil
}

func (s *RetrievablePubSubAgent) StartRetrievableSubscribe(ctx context.Context, topicName string, batchSize, flushInterval uint32) (chan pubsub.RetrievableSubscriptionResults, error) {
	if !s.running {
		return nil, errors.New("not running state")
	}

	ctx, cancel := context.WithCancel(ctx)

	recvCh, errCh, err := s.subscriber.StartTopicSubscription(ctx, topicName, batchSize, flushInterval)
	if err != nil {
		cancel()
		return nil, err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.subscriber.Wait()
		defer cancel()
		for {
			select {
			case err = <-errCh:
				if err != nil {
					logger.Error(err.Error())
				}
				return
			case <-s.shouldQuit:
				logger.Info("stop subscribe from agent stopped")
				return
			}
		}
	}()

	return recvCh, nil
}
