package cmd

import (
	"github.com/spf13/cobra"
	"log"
)

var qagentCmd = &cobra.Command{
	Use:   "qagent [command] (flags)",
	Short: "PaustQ cli",
}

func Main() {

	qagentCmd.AddCommand(
		NewStartCmd(),
	)

	if err := qagentCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
