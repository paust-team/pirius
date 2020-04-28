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
	Port       uint16
	grpcServer *grpc.Server
	db         *storage.QRocksDB
	notifier   *internals.Notifier
}

func NewBroker(port uint16) (*Broker, error) {

	db, err := storage.NewQRocksDB(fmt.Sprintf("qstore-%d", time.Now().UnixNano()), ".")
	if err != nil {
		return nil, err
	}

	notifier := internals.NewNotifier()

	grpcServer := grpc.NewServer()
	paustqproto.RegisterAPIServiceServer(grpcServer, rpc.NewAPIServiceServer(db))
	paustqproto.RegisterStreamServiceServer(grpcServer, rpc.NewStreamServiceServer(db, notifier))

	return &Broker{Port: port, db: db, grpcServer: grpcServer, notifier: notifier}, nil
}

func (b *Broker) Start(ctx context.Context) error {
	go func() {
		select {
		case <-ctx.Done():
			b.Stop()
			return
		}
	}()

	b.notifier.NotifyNews(ctx)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return err
	}

	log.Printf("start broker with port: %d", b.Port)
	if err = b.grpcServer.Serve(lis); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (b *Broker) Stop() {
	b.grpcServer.GracefulStop()
	b.db.Close()
	log.Println("stop broker")
}

func (b *Broker) Clean() {
	_ = b.db.Destroy()
}
