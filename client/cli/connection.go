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

			adminConfig.Load(adminConfigPath)
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

	flags := heartbeatCmd.Flags()
	flags.StringVarP(&echoMsg, "echo-msg", "m", "echotest", "message to ping-pong")
	flags.Uint64VarP(&brokerId, "broker-id", "u", 0, "broker id to send ping")
	flags.StringVarP(&adminConfigPath, "config-path", "i", common.DefaultAdminConfigPath, "admin client config path")
	flags.StringVar(&bootstrapServers, "bootstrap-servers", "", "bootstrap server addresses to connect (ex. localhost:2181)")
	flags.UintVar(&bootstrapTimeout, "bootstrap-timeout", 0, "timeout for bootstrapping")
	flags.IntVar(&timeout, "broker-timeout", 0, "connection timeout (milliseconds)")
	flags.Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")

	adminConfig.BindPFlags(flags)
	adminConfig.BindPFlag("bootstrap.servers", flags.Lookup("bootstrap-servers"))
	adminConfig.BindPFlag("bootstrap.timeout", flags.Lookup("bootstrap-timeout"))

	return heartbeatCmd
}
