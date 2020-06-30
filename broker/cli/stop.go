package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"syscall"
)

func NewStopCmd() *cobra.Command {

	var stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop running shapleq broker",
		Run: func(cmd *cobra.Command, args []string) {
			running, pid := checkRunningBrokerProcess()
			if !running {
				fmt.Println("shapleq broker is not running!")
				os.Exit(1)
			}

			if err := syscall.Kill(pid, syscall.SIGINT); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Println("broker stopped")

		},
	}

	return stopCmd
}
