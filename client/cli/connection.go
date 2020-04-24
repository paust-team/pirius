package cli

import (
	"context"
	"github.com/paust-team/paustq/client"
	"github.com/spf13/cobra"
	"log"
)

var (
	echoMsg  string
	brokerId uint64
)

func NewHeartbeatCmd() *cobra.Command {

	var heartbeatCmd = &cobra.Command{
		Use:   "heartbeat",
		Short: "Send heartbeat to broker",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			apiClient := client.NewAPIClient(ctx, bootstrapServer)
			defer apiClient.Close()

			if apiClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}
			pongMsg, err := apiClient.Heartbeat(echoMsg, brokerId)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("echo msg: %s, server version: %d", pongMsg.Echo, pongMsg.ServerVersion)
		},
	}

	heartbeatCmd.Flags().StringVarP(&echoMsg, "echo-msg", "m", "echotest", "message to ping-pong")
	heartbeatCmd.Flags().Uint64VarP(&brokerId, "broker-id", "i", 0, "broker id to send ping")
	heartbeatCmd.Flags().Uint8VarP(&timeout, "timeout", "t", 5, "set heartbeat timeout(sec)")

	return heartbeatCmd
}
