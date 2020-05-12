package cli

import (
	"fmt"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"os"
)

var (
	zkAddr     string
	logLevel   string
	timeout    uint8
	brokerPort uint16
)

var defaultLogger = logger.NewQLogger("paustq-cli", logger.LogLevelError)

var paustQClientCmd = &cobra.Command{
	Use:   "paustq-client [command] (flags)",
	Short: "Command line interface for PaustQ client",
}

func Main() {

	paustQClientCmd.PersistentFlags().StringVarP(&zkAddr, "zk-addr", "z", "127.0.0.1", "set zookeeper host ip address")
	paustQClientCmd.PersistentFlags().Uint16Var(&brokerPort, "broker-port", common.DefaultBrokerPort, "default broker port")
	paustQClientCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "info", "set log level [debug|info|error|none]")
	paustQClientCmd.PersistentFlags().Uint8VarP(&timeout, "timeout", "t", 3, "set connection timeout(sec)")

	paustQClientCmd.MarkFlagRequired("zk-addr")

	paustQClientCmd.AddCommand(
		NewHeartbeatCmd(),
		NewTopicCmd(),
		NewPublishCmd(),
		NewSubscribeCmd(),
	)

	if err := paustQClientCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
