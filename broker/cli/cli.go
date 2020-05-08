package cli

import (
	"github.com/spf13/cobra"
	"log"
)

var (
	logLevel string
)

var paustQCmd = &cobra.Command{
	Use:   "paustq [command] (flags)",
	Short: "PaustQ cli",
}

func Main() {

	paustQCmd.Flags().StringVar(&logLevel, "log-level", "1", "set log level [0=debug|1=info|2=warning|3=error]")

	paustQCmd.AddCommand(
		NewStartCmd(),
	)

	if err := paustQCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
