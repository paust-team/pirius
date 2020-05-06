package cli

import (
	"github.com/spf13/cobra"
	"log"
	"os"
)

var (
	zkHost		 	string
	logLevel        string
	timeout         uint8
)

var paustQClientCmd = &cobra.Command{
	Use:   "paustq-client [command] (flags)",
	Short: "Command line interface for PaustQ client",
}

func Main() {

	paustQClientCmd.Flags().StringVarP(&zkHost, "zookeeper host", "z", "127.0.0.1", "set zookeeper host ip address")
	paustQClientCmd.Flags().StringVarP(&logLevel, "log-level", "l", "info", "set log level [debug|info|error|none]")
	paustQClientCmd.Flags().Uint8VarP(&timeout, "timeout", "t", 3, "set connection timeout(sec)")

	paustQClientCmd.AddCommand(
		NewHeartbeatCmd(),
		NewCreateTopicCmd(),
		NewListTopicCmd(),
		NewDeleteTopicCmd(),
		NewDescribeTopicCmd(),
		NewPublishCmd(),
		NewSubscribeCmd(),
	)

	if err := paustQClientCmd.Execute(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
