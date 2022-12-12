package agent

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/proto/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"os"
	"sync"
)

type instance struct {
	shouldQuit   chan struct{}
	db           *storage.QRocksDB
	config       config.AgentConfig
	running      bool
	meta         *storage.AgentMeta
	wg           sync.WaitGroup
	coordClient  coordinating.CoordClient
	bootstrapper *bootstrapping.BootstrapService
	grpcServer   *grpc.Server
}

func (s *instance) Start() error {
	if s.config.RetentionPeriod() < constants.MinRetentionPeriod ||
		s.config.RetentionPeriod() > constants.MaxRetentionPeriod {
		logger.Error("Invalid retention period", zap.Uint32("retention", s.config.RetentionPeriod()))
		return errors.New(fmt.Sprintf("Retention period should not be less than %d and should not be greater than %d", constants.MinRetentionPeriod, constants.MaxRetentionPeriod))
	}

	if err := os.MkdirAll(s.config.DataDir(), os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(s.config.LogDir(), os.ModePerm); err != nil {
		return err
	}
	meta, err := storage.LoadAgentMeta(s.GetMetaPath())
	if err != nil {
		logger.Error(err.Error())
		return err
	}
	s.meta = &meta

	db, err := storage.NewQRocksDB(s.config.DBName(), s.config.DataDir())
	if err != nil {
		logger.Error(err.Error())
		return err
	}
	s.db = db
	s.coordClient = helper.BuildCoordClient(s.config.ZKQuorum(), s.config.ZKTimeout())
	if err := s.coordClient.Connect(); err != nil {
		logger.Error(err.Error())
		return err
	}

	s.bootstrapper = bootstrapping.NewBootStrapService(s.coordClient)
	s.running = true
	s.shouldQuit = make(chan struct{})
	s.wg = sync.WaitGroup{}
	logger.Info("agent started with ",
		zap.String("publisher-id", meta.PublisherID),
		zap.String("subscriber-id", meta.SubscriberID),
		zap.Uint("port", s.config.Port()))

	return nil
}

func (s *instance) Stop() {
	close(s.shouldQuit)
	s.running = false
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	// gracefully stop
	s.wg.Wait()
	s.db.Close()
	s.coordClient.Close()
	storage.SaveAgentMeta(s.GetMetaPath(), *s.meta)
	s.grpcServer = nil
	s.bootstrapper = nil
	logger.Info("agent finished")
}

func (s *instance) GetMetaPath() string {
	return s.config.DataDir() + "/" + constants.AgentMetaFileName
}

func (s *instance) GetPublisherID() string {
	return s.meta.PublisherID
}

func (s *instance) GetSubscriberID() string {
	return s.meta.SubscriberID
}

func (s *instance) CleanAllData() {
	logger.Warn("this func made for test purpose only")
	if !s.running {
		_ = s.db.Destroy()
		os.RemoveAll(s.config.LogDir())
		os.RemoveAll(s.config.DataDir())
	}
}

type PubSubAgent struct {
	instance
	subscriber pubsub.Subscriber
	publisher  pubsub.Publisher
}

func NewPubSubAgent(config config.AgentConfig) *PubSubAgent {
	return &PubSubAgent{
		instance: instance{
			config: config,
		},
	}
}
func (s *PubSubAgent) StartWithServer() error {
	if err := s.instance.Start(); err != nil {
		return err
	}
	agentAddress := fmt.Sprintf("%s:%d", s.config.Host(), s.config.Port())
	s.subscriber = pubsub.NewSubscriber(s.meta.SubscriberID, s.bootstrapper, s.meta.SubscribedOffsets)
	s.publisher = pubsub.NewPublisher(s.meta.PublisherID, agentAddress, s.db, s.bootstrapper, s.meta.PublishedOffsets, s.meta.LastFetchedOffset)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterPubSubServer(grpcServer, &s.publisher)

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
		if err = grpcServer.Serve(lis); err != nil {
			logger.Error(err.Error(), zap.String("publisher-id", s.meta.PublisherID))
		} else {
			logger.Info("grpc server stopped", zap.String("publisher-id", s.meta.PublisherID))
		}
		s.running = false
	}()
	return nil
}

func (s *PubSubAgent) Start() error {
	if err := s.instance.Start(); err != nil {
		return err
	}
	agentAddress := fmt.Sprintf("%s:%d", s.config.Host(), s.config.Port())
	s.subscriber = pubsub.NewSubscriber(s.meta.SubscriberID, s.bootstrapper, s.meta.SubscribedOffsets)
	s.publisher = pubsub.NewPublisher(s.meta.PublisherID, agentAddress, s.db, s.bootstrapper, s.meta.PublishedOffsets, s.meta.LastFetchedOffset)

	return nil
}

func (s *PubSubAgent) StartPublish(ctx context.Context, topicName string, sendChan chan pubsub.TopicData) error {
	if !s.running || s.grpcServer == nil {
		return errors.New("not running state")
	}

	retentionPeriod := uint64(s.config.RetentionPeriod() * 60 * 60 * 24)
	ctx, cancel := context.WithCancel(ctx)

	errCh, err := s.publisher.StartTopicPublication(ctx, topicName, retentionPeriod, sendChan)
	if err != nil {
		cancel()
		return err
	}

	//start retention scheduler
	retentionScheduler := storage.NewRetentionScheduler(s.db, s.config.RetentionCheckInterval())
	retentionScheduler.Run(ctx)
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

	return nil
}

func (s *PubSubAgent) StartSubscribe(ctx context.Context, topicName string, batchSize, flushInterval uint32) (chan []pubsub.SubscriptionResult, error) {
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
