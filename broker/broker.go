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

type Broker struct {
	InternalPort 		uint16
	ExternalPort    	uint16
	InternalRPCServer 	*grpc.Server
	ExternalRPCServer 	*grpc.Server
	db         			*storage.QRocksDB
	notifier   			*internals.Notifier
}

func NewBroker(port uint16) (*Broker, error) {

	db, err := storage.NewQRocksDB(fmt.Sprintf("qstore-%d", time.Now().UnixNano()), ".")
	if err != nil {
		return nil, err
	}

	notifier := internals.NewNotifier()

	externalRPCServer := grpc.NewServer()
	paustqproto.RegisterAPIServiceServer(externalRPCServer, rpc.NewAPIServiceServer(db))
	paustqproto.RegisterStreamServiceServer(externalRPCServer, rpc.NewStreamServiceServer(db, notifier))

	internalRPCServer := grpc.NewServer()
	paustqproto.RegisterStreamServiceServer(internalRPCServer, rpc.NewStreamServiceServer(db, notifier))

	var defaultInternalPort uint16 = 11010

	return &Broker{InternalPort: defaultInternalPort, ExternalPort: port, db: db, ExternalRPCServer: externalRPCServer, InternalRPCServer: internalRPCServer, notifier: notifier}, nil
}

func (b *Broker) Start(ctx context.Context) error {

	defer b.stop()

	errChan := make(chan error)
	defer close(errChan)

	b.notifier.NotifyNews(ctx, errChan)

	startGrpcServer := func(server *grpc.Server, port uint16, errChan chan error) {

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

	go startGrpcServer(b.InternalRPCServer, b.InternalPort, errChan)
	go startGrpcServer(b.ExternalRPCServer, b.ExternalPort, errChan)

	log.Printf("start broker with port: %d", b.ExternalPort)

	select {
	case <-ctx.Done():
		return nil
	case err := <- errChan:
		log.Println(err)
		return err
	}
}

func (b *Broker) stop() {
	b.InternalRPCServer.GracefulStop()
	b.ExternalRPCServer.GracefulStop()
	b.db.Close()
	log.Println("stop broker")
}

func (b *Broker) Clean() {
	_ = b.db.Destroy()
}
