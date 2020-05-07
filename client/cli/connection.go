package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/spf13/cobra"
	"log"
	"time"
)

var (
	echoMsg  	string
	brokerId 	uint64
	brokerHost 	string
)

func NewHeartbeatCmd() *cobra.Command {

	var heartbeatCmd = &cobra.Command{
		Use:   "heartbeat",
		Short: "Send heartbeat to broker",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			rpcClient := client.NewRPCClient(zkAddr).WithTimeout(time.Duration(timeout) * time.Second)
			defer rpcClient.Close()

			if rpcClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}
			brokerEndpoint := fmt.Sprintf("%s:%d", brokerHost, brokerPort)
			pongMsg, err := rpcClient.Heartbeat(ctx, echoMsg, brokerId, brokerEndpoint)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("echo msg: %s, server version: %d", pongMsg.Echo, pongMsg.ServerVersion)
		},
	}

	heartbeatCmd.Flags().StringVarP(&echoMsg, "echo-msg", "m", "echotest", "message to ping-pong")
	heartbeatCmd.Flags().StringVarP(&brokerHost, "broker-addr", "b", "127.0.0.1", "broker address")
	heartbeatCmd.Flags().Uint64VarP(&brokerId, "broker-id", "i", 0, "broker id to send ping")
	heartbeatCmd.Flags().Uint8VarP(&timeout, "timeout", "t", 5, "set heartbeat timeout(sec)")

	heartbeatCmd.MarkFlagRequired("broker-addr")

	return heartbeatCmd
}
