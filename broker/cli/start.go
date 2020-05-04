package cli

import (
	"context"
	"github.com/paust-team/paustq/broker"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var (
	dir  string
	port uint16
)

func NewStartCmd() *cobra.Command {

	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start paustq broker",
		Run: func(cmd *cobra.Command, args []string) {
			brokerInstance, err := broker.NewBroker(port)
			if err != nil {
				log.Fatal(err)
			}

			if err := brokerInstance.Start(context.Background()); err != nil {
				log.Fatal(err)
			}
		},
	}

	startCmd.Flags().StringVarP(&dir, "dir", "d", os.ExpandEnv("$HOME/.paustq"), "directory for data store")
	startCmd.Flags().Uint16VarP(&port, "port", "p", 9000, "external port")

	return startCmd
}
