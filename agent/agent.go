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
	"go.uber.org/zap"
	"os"
	"sync"
)

type Instance struct {
	shouldQuit  chan struct{}
	db          *storage.QRocksDB
	config      config.AgentConfig
	running     bool
	subscriber  pubsub.Subscriber
	publisher   pubsub.Publisher
	meta        *storage.AgentMeta
	wg          sync.WaitGroup
	coordClient coordinating.CoordClient
}

func NewInstance(config config.AgentConfig) *Instance {
	return &Instance{
		config:  config,
		running: false,
		wg:      sync.WaitGroup{},
	}
}

func (s *Instance) Start() error {
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
	agentAddress := fmt.Sprintf("%s:%d", s.config.Host(), s.config.Port())
	bootstrapper := bootstrapping.NewBootStrapService(s.coordClient)
	s.subscriber = pubsub.NewSubscriber(meta.SubscriberID, bootstrapper, s.meta.SubscribedOffsets)
	s.publisher = pubsub.NewPublisher(s.meta.PublisherID, agentAddress, db, bootstrapper, s.meta.PublishedOffsets, s.meta.LastFetchedOffset)
	s.running = true
	s.shouldQuit = make(chan struct{})
	logger.Info("agent started with ",
		zap.String("publisher-id", meta.PublisherID),
		zap.String("subscriber-id", meta.SubscriberID),
		zap.Uint("port", s.config.Port()))

	return nil
}

func (s *Instance) Stop() {
	close(s.shouldQuit)
	s.running = false
	// gracefully stop
	s.wg.Wait()
	s.db.Close()
	s.coordClient.Close()
	storage.SaveAgentMeta(s.GetMetaPath(), *s.meta)
	logger.Info("agent finished")
}

func (s *Instance) StartPublish(ctx context.Context, topicName string, sendChan chan pubsub.TopicData) error {
	if !s.running {
		return errors.New("not running state")
	}
	retentionPeriod := uint64(s.config.RetentionPeriod() * 60 * 60 * 24)
	ctx, cancel := context.WithCancel(ctx)

	errCh, err := s.publisher.PreparePublication(ctx, topicName, retentionPeriod, sendChan)
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

func (s *Instance) StartSubscribe(ctx context.Context, topicName string, batchSize, flushInterval uint32) (chan []pubsub.SubscriptionResult, error) {
	if !s.running {
		return nil, errors.New("not running state")
	}
	ctx, cancel := context.WithCancel(ctx)

	recvCh, errCh, err := s.subscriber.PrepareSubscription(ctx, topicName, batchSize, flushInterval)
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

func (s *Instance) GetMetaPath() string {
	return s.config.DataDir() + "/" + constants.AgentMetaFileName
}

func (s *Instance) GetPublisherID() string {
	return s.meta.PublisherID
}

func (s *Instance) GetSubscriberID() string {
	return s.meta.SubscriberID
}

func (s *Instance) CleanAllData() {
	logger.Warn("this func made for test purpose only")
	if !s.running {
		_ = s.db.Destroy()
		os.RemoveAll(s.config.LogDir())
		os.RemoveAll(s.config.DataDir())
	}
}
