package agent

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/logger"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/agent/utils"
	"go.uber.org/zap"
	"io"
	"os"
)

type ShapleQAgent struct {
	shouldQuit chan struct{}
	db         *storage.QRocksDB
	config     *config.AgentConfig
	running    bool
	subscriber *pubsub.Subscriber
	publisher  *pubsub.Publisher
}

func NewShapleQAgent(config *config.AgentConfig) *ShapleQAgent {
	return &ShapleQAgent{
		shouldQuit: make(chan struct{}),
		config:     config,
		running:    false,
	}
}

func (s *ShapleQAgent) Start() error {

	if s.config.RetentionPeriod() < utils.MinRetentionPeriod ||
		s.config.RetentionPeriod() > utils.MaxRetentionPeriod {
		logger.Error("Invalid retention period", zap.Uint32("retention", s.config.RetentionPeriod()))
		return errors.New(fmt.Sprintf("Retention period should not be less than %d and should not be greater than %d", utils.MinRetentionPeriod, utils.MaxRetentionPeriod))
	}

	if err := os.MkdirAll(s.config.DataDir(), os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(s.config.LogDir(), os.ModePerm); err != nil {
		return err
	}

	db, err := storage.NewQRocksDB(s.config.DBName(), s.config.DataDir())
	if err != nil {
		logger.Error(err.Error())
		return err
	}
	s.db = db
	s.subscriber = &pubsub.Subscriber{}
	s.publisher = &pubsub.Publisher{
		DB: db,
	}
	s.running = true
	logger.Info("agent started with ", zap.Uint("port", s.config.Port()))

	return nil
}

func (s *ShapleQAgent) Stop() {
	close(s.shouldQuit)
	s.db.Close()
	s.running = false
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

	errCh, err := s.publisher.InitTopicStream(ctx, topicName, retentionPeriod, sendChan)
	if err != nil {
		cancel()
		return err
	}

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

	recvChan, errCh, err := s.subscriber.RegisterSubscription(ctx, topicName, batchSize, flushInterval)
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

	return recvChan, nil
}
