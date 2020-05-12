package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

func NewStatusCmd() *cobra.Command {

	var statusCmd = &cobra.Command{
		Use:   "status",
		Short: "show status of paustq broker",
		Run: func(cmd *cobra.Command, args []string) {
			running, pid := checkRunningBrokerProcess()
			if running {
				fmt.Printf("running with port %d", pid)
			} else {
				fmt.Println("not running")
			}
		},
	}

	return statusCmd
}
