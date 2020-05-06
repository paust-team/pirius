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
	"time"
)

type Broker struct {
	Port      			uint16
	host				string
	grpcServer 			*grpc.Server
	db                	*storage.QRocksDB
	notifier          	*internals.Notifier
	zkClient 			*zookeeper.ZKClient
}

func NewBroker(zkAddr string) (*Broker, error) {

	db, err := storage.NewQRocksDB(fmt.Sprintf("qstore-%d", time.Now().UnixNano()), ".")
	if err != nil {
		return nil, err
	}

	notifier := internals.NewNotifier()
	zkClient := zookeeper.NewZKClient(zkAddr)

	return &Broker{Port: common.DefaultBrokerPort, db: db, notifier: notifier, zkClient:zkClient}, nil
}

func (b *Broker) WithPort(port uint16) *Broker{
	b.Port = port
	return b
}

func (b *Broker) Start(ctx context.Context) error {

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
	case err := <- errChan:
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
}
