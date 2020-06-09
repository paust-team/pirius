package cli

import (
	"fmt"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"os"
)

var (
	brokerAddr string
	logLevel   string
	timeout    uint
)

var defaultLogger = logger.NewQLogger("paustq-cli", logger.Error)

var paustQClientCmd = &cobra.Command{
	Use:   "paustq-client [command] (flags)",
	Short: "Command line interface for PaustQ client",
}

func Main() {
	paustQClientCmd.PersistentFlags().StringVarP(&brokerAddr, "broker-addr", "a", "127.0.0.1:1101", "set broker address (ip:port)")
	paustQClientCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "info", "set log level [debug|info|error|none]")
	paustQClientCmd.PersistentFlags().UintVarP(&timeout, "timeout", "t", 3, "set connection timeout(sec)")

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
