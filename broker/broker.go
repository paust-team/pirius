package broker

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/rpc"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/common"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

var (
	DefaultLogDir = os.ExpandEnv("$HOME/.paustq/log")
	DefaultDataDir = os.ExpandEnv("$HOME/.paustq/data")
)

type Broker struct {
	Port       uint16
	host       string
	grpcServer *grpc.Server
	db         *storage.QRocksDB
	notifier   *internals.Notifier
	zkClient   *zookeeper.ZKClient
	logDir 		string
	dataDir 	string
}

func NewBroker(zkAddr string) (*Broker, error) {

	notifier := internals.NewNotifier()
	zkClient := zookeeper.NewZKClient(zkAddr)

	return &Broker{Port: common.DefaultBrokerPort, notifier: notifier, zkClient: zkClient,
		logDir: DefaultLogDir, dataDir: DefaultDataDir}, nil
}

func (b *Broker) WithPort(port uint16) *Broker {
	b.Port = port
	return b
}

func (b *Broker) WithLogDir(dir string) *Broker {
	b.logDir = dir
	return b
}

func (b *Broker) WithDataDir(dir string) *Broker {
	b.dataDir = dir
	return b
}

func (b *Broker) Start(ctx context.Context) error {

	// create directories
	if err := os.MkdirAll(b.dataDir, os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(b.logDir, os.ModePerm); err != nil {
		return err
	}

	db, err := storage.NewQRocksDB(fmt.Sprintf("qstore-%d", time.Now().UnixNano()), b.dataDir)
	if err != nil {
		return err
	}
	b.db = db

	// start grpc server
	b.grpcServer = grpc.NewServer()
	host := zookeeper.GetOutboundIP()
	if !zookeeper.IsPublicIP(host) {
		log.Println("cannot attach to broker from external network")
	}

	b.host = host.String()
	if err := b.zkClient.Connect(); err != nil {
		return err
	}

	if err := b.zkClient.CreatePathsIfNotExist(); err != nil {
		return err
	}

	if err := b.zkClient.AddBroker(b.host); err != nil {
		return err
	}

	paustqproto.RegisterAPIServiceServer(b.grpcServer, rpc.NewAPIServiceServer(b.db, b.zkClient))
	paustqproto.RegisterStreamServiceServer(b.grpcServer, rpc.NewStreamServiceServer(b.db, b.notifier, b.zkClient, b.host))

	defer b.Stop()

	errChan := make(chan error)
	defer close(errChan)

	b.notifier.NotifyNews(ctx, errChan)

	startGrpcServer := func(server *grpc.Server, port uint16) {

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log.Printf("failed to listen: %v", err)
			errChan <- err
			return
		}

		if err = server.Serve(lis); err != nil {
			log.Println(err)
			errChan <- err
		}
	}

	go startGrpcServer(b.grpcServer, b.Port)

	log.Printf("start broker with port: %d", b.Port)

	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		log.Println(err)
		return err
	}
}

func (b *Broker) Stop() {
	b.grpcServer.GracefulStop()
	b.db.Close()
	b.zkClient.RemoveBroker(b.host)
	topics, _ := b.zkClient.GetTopics()
	for _, topic := range topics {
		b.zkClient.RemoveTopicBroker(topic, b.host)
	}
	b.zkClient.Close()
	log.Println("broker stopped")
}

func (b *Broker) Clean() {
	_ = b.db.Destroy()
	os.RemoveAll(b.logDir)
	os.RemoveAll(b.dataDir)
}
