package broker

import (
	"fmt"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/rpc"
	"github.com/paust-team/paustq/broker/storage"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Broker struct {
	Port 			uint16
	grpcServer 		*grpc.Server
	db 				*storage.QRocksDB
}

func NewBroker(port uint16) *Broker {

	db, err := storage.NewQRocksDB("qstore", ".")
	topic := internals.NewTopic()

	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	paustqproto.RegisterTopicServiceServer(grpcServer, rpc.NewTopicServiceServer(db))
	paustqproto.RegisterStreamServiceServer(grpcServer, rpc.NewStreamServiceServer(db, topic))

	return &Broker{Port: port, db: db, grpcServer: grpcServer,}
}

func (b *Broker) Start() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("start broker with port: %d", b.Port)
	if err = b.grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func (b *Broker) Stop() {
	b.grpcServer.Stop()
	b.db.Close()
	log.Println("stop broker")
}
