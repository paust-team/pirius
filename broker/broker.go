package broker

import (
	"fmt"
	"github.com/paust-team/paustq/broker/rpc"
	"github.com/paust-team/paustq/broker/storage"
	paustqproto "github.com/paust-team/paustq/proto"
	"google.golang.org/grpc"
	"log"
	"net"
)

func StartBroker(port uint16) {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	db, err := storage.NewQRocksDB("qstore", ".")
	defer db.Close()

	if err != nil {
		log.Fatal(err)
		return
	}

	grpcServer := grpc.NewServer()
	paustqproto.RegisterTopicServiceServer(grpcServer, rpc.NewTopicServiceServer(db))
	paustqproto.RegisterStreamServiceServer(grpcServer, rpc.NewStreamServiceServer(db))

	log.Printf("Start broker with port: %d", port)
	if err = grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}