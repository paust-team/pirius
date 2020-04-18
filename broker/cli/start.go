package cli

import (
	"github.com/paust-team/paustq/broker"
	"github.com/spf13/cobra"
	"os"
)

var (
	dir 		string
	port 		uint16
)

func NewStartCmd() *cobra.Command {

	var startCmd = &cobra.Command{
		Use: "start",
		Short: "start paustq broker",
		Run: func(cmd *cobra.Command, args []string) {
			broker.StartBroker(port)
		},
	}

	startCmd.Flags().StringVarP(&dir, "dir", "d", os.ExpandEnv("$HOME/.paustq"), "directory for data store")
	startCmd.Flags().Uint16VarP(&port, "port", "p", 1101, "directory for data store")

	return startCmd
}
