package broker

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/broker/rebalancing"
	"github.com/paust-team/shapleq/broker/rpc"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"sync"
)

type Instance struct {
	rpc.TopicService
	config      config.BrokerConfig
	running     bool
	server      *grpc.Server
	coordClient coordinating.CoordClient
	wg          sync.WaitGroup
	rebalancer  rebalancing.Rebalancer
}

func NewInstance(config config.BrokerConfig) *Instance {
	return &Instance{
		TopicService: rpc.TopicService{},
		config:       config,
		running:      false,
		wg:           sync.WaitGroup{},
	}
}

func (s *Instance) Start() error {
	logger.Info("starting a broker")
	s.coordClient = helper.BuildCoordClient(s.config.ZKQuorum(), s.config.ZKTimeout())
	if err := s.coordClient.Connect(); err != nil {
		logger.Error(err.Error())
		return err
	}

	if err := path.CreatePathsIfNotExist(s.coordClient); err != nil {
		logger.Error(err.Error())
		return err
	}

	// add broker path
	bootstrapper := bootstrapping.NewBootStrapService(s.coordClient)
	brokerHost := fmt.Sprintf("%s:%d", s.config.Host(), s.config.Port())
	if err := bootstrapper.AddBroker(brokerHost); err != nil {
		logger.Error("error on adding a broker", zap.Error(err))
		return err
	}

	// run rebalancer
	ctx, cancel := context.WithCancel(context.Background())
	s.rebalancer = rebalancing.NewRebalancer(bootstrapper, brokerHost)
	if err := s.rebalancer.Run(ctx); err != nil {
		logger.Error("error on starting rebalancer", zap.Error(err))
		cancel()
		return err
	}

	// run gRPC server
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterTopicServer(grpcServer, s)
	s.server = grpcServer
	s.TopicService = rpc.NewTopicService(bootstrapper.CoordClientTopicWrapper)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.BindAddress(), s.config.Port()))
	logger.Debug("grpc server listening", zap.String("bind", s.config.BindAddress()), zap.Uint("port", s.config.Port()))
	if err != nil {
		cancel()
		return err
	}

	s.running = true
	logger.Info("broker is started")
	s.wg.Add(1)
	defer s.wg.Done()
	defer s.rebalancer.Wait()
	defer cancel()
	if err = grpcServer.Serve(lis); err != nil {
		logger.Error(err.Error())
	}

	return err
}

func (s *Instance) Stop() {
	logger.Info("stopping a broker")
	s.server.Stop()
	// gracefully stop
	s.wg.Wait()
	s.running = false
	s.coordClient.Close()
	logger.Info("broker is stopped")
}

// RPC overrides

// CreateTopic : when create topic called, fragment-rebalancing should be triggered
func (s *Instance) CreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.Empty, error) {
	if !s.running {
		return nil, qerror.InvalidStateError{State: "broker is not running"}
	}
	res, err := s.TopicService.CreateTopic(ctx, request)
	if err != nil {
		return nil, err
	}
	if err = s.rebalancer.RegisterTopicWatchers(request.GetName()); err != nil {
		return nil, err
	}
	return res, err
}

// DeleteTopic : when delete topic called, fragment-rebalancing should be triggered
func (s *Instance) DeleteTopic(ctx context.Context, request *pb.TopicRequestWithName) (*pb.Empty, error) {
	if !s.running {
		return nil, qerror.InvalidStateError{State: "broker is not running"}
	}
	if err := s.rebalancer.DeregisterTopicWatchers(request.GetName()); err != nil {
		return nil, err
	}

	res, err := s.TopicService.DeleteTopic(ctx, request)
	if err != nil {
		return nil, err
	}
	return res, err
}
