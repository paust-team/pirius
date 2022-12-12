package cli

import (
	"github.com/spf13/cobra"
	"log"
)

var qagentCmd = &cobra.Command{
	Use:   "qagent [command] (flags)",
	Short: "ShapleQ Agent cli",
}

func Main() {

	qagentCmd.AddCommand(
		NewStartPublishCmd(),
		NewStartSubscribeCmd(),
		NewTopicCmd(),
	)

	if err := qagentCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
