package cli

import (
	"github.com/spf13/cobra"
	"log"
)

var paustQCmd = &cobra.Command{
	Use:   "paustq [command] (flags)",
	Short: "PaustQ cli",
}

func Main() {

	paustQCmd.AddCommand(
		NewStartCmd(),
	)

	if err := paustQCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
