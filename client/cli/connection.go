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

	adminConfig := config.NewAdminConfig()

	var heartbeatCmd = &cobra.Command{
		Use:   "heartbeat",
		Short: "Send heartbeat to broker",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(configPath)
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
	heartbeatCmd.Flags().Uint64VarP(&brokerId, "broker-id", "u", 0, "broker id to send ping")
	heartbeatCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultAdminConfigPath, "admin client config path")
	heartbeatCmd.Flags().StringVar(&brokerHost, "broker-host", "", "broker host")
	heartbeatCmd.Flags().UintVar(&brokerPort, "broker-port", 0, "broker port")
	heartbeatCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	heartbeatCmd.Flags().IntVar(&timeout, "timeout", 0, "connection timeout (seconds)")

	adminConfig.BindPFlags(heartbeatCmd.Flags())
	adminConfig.BindPFlag("broker.host", heartbeatCmd.Flags().Lookup("broker-host"))
	adminConfig.BindPFlag("broker.port", heartbeatCmd.Flags().Lookup("broker-port"))

	return heartbeatCmd
}
