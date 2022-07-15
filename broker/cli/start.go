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
	configPath string
	logDir     string
	dataDir    string
	logLevel   uint8
	port       uint
	zkQuorum   string
	zkTimeout  uint
	timeout    uint
	hostname   string
)

func NewStartCmd() *cobra.Command {

	brokerConfig := config.NewBrokerConfig()
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
				brokerConfig.Load(configPath)
				brokerInstance := broker.NewBroker(brokerConfig)

				if cmd.Flags().Changed("clear") {
					defer brokerInstance.Clean()
				}
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
	startCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultBrokerConfigPath, "broker config directory")
	startCmd.Flags().StringVar(&logDir, "log-dir", "", "log directory")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "", "data directory")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().UintVar(&port, "port", 0, "broker port")
	startCmd.Flags().StringVar(&zkQuorum, "zk-quorum", "", "zookeeper quorum")
	startCmd.Flags().UintVar(&zkTimeout, "zk-timeout", 0, "zookeeper timeout")
	startCmd.Flags().UintVar(&timeout, "timeout", 10000, "shapleq client timeout")
	startCmd.Flags().StringVar(&hostname, "hostname", "localhost", "shapleq broker hostname for registered bootstrapping")
	startCmd.Flags().BoolP("clear", "c", false, "DANGER: use this option only if you intend to reset data directory after broker is terminated")

	brokerConfig.BindPFlags(startCmd.Flags())
	brokerConfig.BindPFlag("zookeeper.quorum", startCmd.Flags().Lookup("zk-quorum"))
	brokerConfig.BindPFlag("zookeeper.timeout", startCmd.Flags().Lookup("zk-timeout"))

	return startCmd
}
