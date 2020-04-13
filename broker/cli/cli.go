package cli

import (
	"github.com/spf13/cobra"
	"log"
	"os"
)

var (
	logLevel 	string
)

var paustQCmd = &cobra.Command{
	Use: "paustq [command] (flags)",
	Short: "PaustQ cli",
}

func Main() {

	paustQCmd.Flags().StringVarP(&logLevel, "log-level", "l", "info", "set log level [debug|info|error|none]")

	paustQCmd.AddCommand(
		NewStartCmd(),
		)

	if err := paustQCmd.Execute(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
