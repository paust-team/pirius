package cli

import (
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

var (
	configDir string
)

func NewStartCmd() *cobra.Command {

	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start shapleq broker",
		Run: func(cmd *cobra.Command, args []string) {
			running, pid := checkRunningBrokerProcess()
			if running {
				fmt.Printf("broker already running on port %d", pid)
				return
			}

			if cmd.Flags().Changed("daemon") {

				reconstructedArgs := []string{"start"}
				cmd.Flags().Visit(func(flag *pflag.Flag) {
					if flag.Name != "daemon" {
						reconstructedArgs = append(reconstructedArgs, fmt.Sprintf("--%s", flag.Name), flag.Value.String())
					}
				})

				dir, err := os.Getwd()
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				daemon := exec.Command(fmt.Sprintf("%s/shapleq", dir), reconstructedArgs...)
				if err = daemon.Start(); err != nil {
					fmt.Println("start error: ", err)
					os.Exit(1)
				}

				go func() {
					err := daemon.Wait()
					fmt.Println(err)
				}()
				fmt.Printf("run broker on background with process id %d", daemon.Process.Pid)
				return
			} else {
				brokerConfig := config.NewBrokerConfig().Load(configDir)
				brokerInstance := broker.NewBroker(brokerConfig)

				sigCh := make(chan os.Signal, 1)
				defer close(sigCh)
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				done := make(chan bool)
				defer close(done)

				go func() {
					brokerInstance.Start()
					done <- true
				}()

				for {
					select {
					case <-done:
						fmt.Println("broker process finished")
						return
					case sig := <-sigCh:
						fmt.Println("received signal:", sig)
						brokerInstance.Stop()
					}
				}
			}
		},
	}

	startCmd.Flags().BoolP("daemon", "d", false, "run with daemon")
	startCmd.Flags().StringVarP(&configDir, "config-dir", "p", common.DefaultBrokerConfigDir, "broker config directory")

	return startCmd
}
