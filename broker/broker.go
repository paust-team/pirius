package broker

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/rpc"
	"github.com/paust-team/paustq/broker/storage"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)
var DefaultInternalPort uint16 = 11010

type Broker struct {
	internalPort 		uint16
	externalPort    	uint16
	internalRPCServer 	*grpc.Server
	externalRPCServer 	*grpc.Server
	db         			*storage.QRocksDB
	notifier   			*internals.Notifier
	zkHelper 			internals.ZookeeperHelper
}

func NewBroker(port uint16) (*Broker, error) {

	db, err := storage.NewQRocksDB(fmt.Sprintf("qstore-%d", time.Now().UnixNano()), ".")
	if err != nil {
		return nil, err
	}

	notifier := internals.NewNotifier()
	zkHelper := internals.NewZookeeperHelper()

	return &Broker{internalPort: DefaultInternalPort, externalPort: port, db: db, zkHelper: zkHelper, notifier: notifier}, nil
}

func (b *Broker) WithInternalPort(port uint16) *Broker{
	b.internalPort = port
	return b
}

func (b *Broker) WithZkHelper(zkHelper internals.ZookeeperHelper) *Broker{
	b.zkHelper = zkHelper
	return b
}

func (b *Broker) Start(ctx context.Context) error {

	b.externalRPCServer = grpc.NewServer()
	paustqproto.RegisterAPIServiceServer(b.externalRPCServer, rpc.NewAPIServiceServer(b.db))
	paustqproto.RegisterStreamServiceServer(b.externalRPCServer, rpc.NewStreamServiceServer(b.db, b.notifier, b.zkHelper))

	b.internalRPCServer = grpc.NewServer()
	paustqproto.RegisterStreamServiceServer(b.internalRPCServer, rpc.NewStreamServiceServer(b.db, b.notifier, b.zkHelper))

	defer b.stop()

	errChan := make(chan error)
	defer close(errChan)

	b.notifier.NotifyNews(ctx, errChan)

	startGrpcServer := func(server *grpc.Server, port uint16) {

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log.Printf("failed to listen: %v", err)
			errChan <- err
		}

		if err = server.Serve(lis); err != nil {
			log.Println(err)
			errChan <- err
		}
	}

	go startGrpcServer(b.internalRPCServer, b.internalPort)
	go startGrpcServer(b.externalRPCServer, b.externalPort)

	log.Printf("start broker with port: %d", b.externalPort)

	select {
	case <-ctx.Done():
		return nil
	case err := <- errChan:
		log.Println(err)
		return err
	}
}

func (b *Broker) stop() {
	b.internalRPCServer.GracefulStop()
	b.externalRPCServer.GracefulStop()
	b.db.Close()
	log.Println("stop broker")
}

func (b *Broker) Clean() {
	_ = b.db.Destroy()
}
