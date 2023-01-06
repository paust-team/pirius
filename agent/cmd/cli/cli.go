package cli

import (
	"github.com/spf13/cobra"
	"log"
)

var agentCmd = &cobra.Command{
	Use:   "pirius-agent [command] (flags)",
	Short: "Pirius Agent cli",
}

func Main() {

	agentCmd.AddCommand(
		NewStartPublishCmd(),
		NewStartSubscribeCmd(),
		NewTopicCmd(),
	)

	if err := agentCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
