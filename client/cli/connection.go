package cli

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	"github.com/spf13/cobra"
	"os"
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

			adminConfig := config.NewAdminConfig()
			adminConfig.Load(common.DefaultAdminConfigDir)
			adminClient := client.NewAdmin(adminConfig)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			pongMsg, err := adminClient.Heartbeat(echoMsg, brokerId)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			fmt.Printf("echo msg: %s, server version: %d", pongMsg.Echo, pongMsg.ServerVersion)
		},
	}

	heartbeatCmd.Flags().StringVarP(&echoMsg, "echo-msg", "m", "echotest", "message to ping-pong")
	heartbeatCmd.Flags().Uint64VarP(&brokerId, "broker-id", "i", 0, "broker id to send ping")
	heartbeatCmd.Flags().StringVarP(&configDir, "config-dir", "c", common.DefaultAdminConfigDir, "admin client config directory")

	return heartbeatCmd
}
