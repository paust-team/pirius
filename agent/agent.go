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
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"go.uber.org/zap"
	"io"
	"os"
)

type ShapleQAgent struct {
	shouldQuit chan struct{}
	db         *storage.QRocksDB
	config     config.AgentConfig
	running    bool
	subscriber pubsub.Subscriber
	publisher  pubsub.Publisher
	meta       *storage.AgentMeta
}

func NewShapleQAgent(config config.AgentConfig) *ShapleQAgent {
	return &ShapleQAgent{
		shouldQuit: make(chan struct{}),
		config:     config,
		running:    false,
	}
}

func (s *ShapleQAgent) Start() error {
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
	meta, err := storage.LoadAgentMeta(s.config.DataDir() + "/" + constants.AgentMetaFileName)
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
	bootstrapper := bootstrapping.NewBootStrapService(helper.BuildCoordClient(s.config))
	s.subscriber = pubsub.Subscriber{Bootstrapper: bootstrapper, LastFragmentOffsets: s.meta.SubscribedOffsets}
	s.publisher = pubsub.Publisher{DB: db, Bootstrapper: bootstrapper, NextFragmentOffsets: s.meta.PublishedOffsets}
	s.running = true
	logger.Info("agent started with ",
		zap.String("publisher-id", meta.PublisherID),
		zap.String("subscriber-id", meta.SubscriberID),
		zap.Uint("port", s.config.Port()))

	return nil
}

func (s *ShapleQAgent) Stop() {
	close(s.shouldQuit)
	s.db.Close()
	s.running = false
	storage.SaveAgentMeta(s.config.DataDir()+"/"+constants.AgentMetaFileName, *s.meta)
	logger.Info("agent finished")
}

func (s *ShapleQAgent) StartPublish(topicName string, sendChan chan pubsub.TopicData) error {
	if !s.running {
		return errors.New("not running state")
	}
	retentionPeriod := uint64(s.config.RetentionPeriod() * 60 * 60 * 24)
	ctx, cancel := context.WithCancel(context.Background())

	if err := s.publisher.SetupGrpcServer(ctx, s.config.BindAddress(), s.config.Port()); err != nil {
		cancel()
		return err
	}

	errCh, err := s.publisher.PreparePublication(ctx, topicName, retentionPeriod, sendChan)
	if err != nil {
		cancel()
		return err
	}

	//start retention scheduler
	retentionScheduler := storage.NewRetentionScheduler(s.db, s.config.RetentionCheckInterval())
	retentionScheduler.Run(ctx)

	go func() {
		defer cancel()
		for {
			select {
			case err = <-errCh:
				logger.Error(err.Error())
				return
			case <-s.shouldQuit:
				logger.Info("stop publish from agent stopped")
				return
			}
		}
	}()

	return nil
}

func (s *ShapleQAgent) StartSubscribe(topicName string, batchSize, flushInterval uint32) (chan []pubsub.SubscriptionResult, error) {
	if !s.running {
		return nil, errors.New("not running state")
	}
	ctx, cancel := context.WithCancel(context.Background())

	recvCh, errCh, err := s.subscriber.PrepareSubscription(ctx, topicName, batchSize, flushInterval)
	if err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer cancel()
		for {
			select {
			case err, ok := <-errCh:
				if !ok {
					return
				}
				if err == io.EOF {
					// TODO :: this is abnormal case. should be restarted?
					logger.Info("stop subscribe from io.EOF")
					return
				}
				logger.Error(err.Error())
				return
			case <-s.shouldQuit:
				logger.Info("stop subscribe from agent stopped")
				return
			}
		}
	}()

	return recvCh, nil
}
