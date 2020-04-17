package cli

import (
	"fmt"
	"github.com/paust-team/paustq/broker/rpc"
	"github.com/paust-team/paustq/broker/storage"
	paustq_proto "github.com/paust-team/paustq/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

var (
	dir 		string
	port 		uint16
)

func NewStartCmd() *cobra.Command {

	var startCmd = &cobra.Command{
		Use: "start",
		Short: "start paustq broker",
		Run: func(cmd *cobra.Command, args []string) {
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
			paustq_proto.RegisterTopicServiceServer(grpcServer, rpc.NewTopicServiceServer(db))
			paustq_proto.RegisterPubSubServiceServer(grpcServer, rpc.NewPubSubServiceServer(db))

			if err = grpcServer.Serve(lis); err != nil {
				log.Fatal(err)
			}
		},
	}

	startCmd.Flags().StringVarP(&dir, "dir", "d", os.ExpandEnv("$HOME/.paustq"), "directory for data store")
	startCmd.Flags().Uint16VarP(&port, "port", "p", 1101, "directory for data store")

	return startCmd
}
