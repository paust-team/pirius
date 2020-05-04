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
	Port      			uint16
	grpcServer 			*grpc.Server
	db                	*storage.QRocksDB
	notifier          	*internals.Notifier
}

func NewBroker(port uint16) (*Broker, error) {

	db, err := storage.NewQRocksDB(fmt.Sprintf("qstore-%d", time.Now().UnixNano()), ".")
	if err != nil {
		return nil, err
	}

	notifier := internals.NewNotifier()

	return &Broker{Port: port, db: db, notifier: notifier}, nil
}

func (b *Broker) Start(ctx context.Context) error {

	b.grpcServer = grpc.NewServer()
	paustqproto.RegisterAPIServiceServer(b.grpcServer, rpc.NewAPIServiceServer(b.db))
	paustqproto.RegisterStreamServiceServer(b.grpcServer, rpc.NewStreamServiceServer(b.db, b.notifier))

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
	log.Println("stop broker")
}

func (b *Broker) Clean() {
	_ = b.db.Destroy()
}
