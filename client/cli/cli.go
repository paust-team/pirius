package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var configPath string

var shapleQClientCmd = &cobra.Command{
	Use:   "shapleq-client [command] (flags)",
	Short: "Command line interface for PaustQ client",
}

func Main() {

	shapleQClientCmd.AddCommand(
		NewHeartbeatCmd(),
		NewTopicCmd(),
		NewPublishCmd(),
		NewSubscribeCmd(),
	)

	if err := shapleQClientCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
