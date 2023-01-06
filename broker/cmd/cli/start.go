package cli

import (
	"fmt"
	"github.com/paust-team/pirius/broker"
	"github.com/paust-team/pirius/broker/config"
	"github.com/paust-team/pirius/constants"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"log"
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
	zkQuorum   []string
	zkTimeout  uint
	hostname   string
	bind       string
)

func NewStartCmd() *cobra.Command {

	brokerConfig := config.NewBrokerConfig()
	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start pirius broker",
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

				daemon := exec.Command(fmt.Sprintf("%s/pirius-broker", dir), reconstructedArgs...)
				if err = daemon.Start(); err != nil {
					fmt.Println("start error: ", err)
					os.Exit(1)
				}

				go func() {
					if err := daemon.Wait(); err != nil {
						log.Fatal(err)
					}
				}()
				fmt.Printf("run broker on background with process id %d", daemon.Process.Pid)
				return
			} else {
				brokerConfig.Load(configPath)
				brokerInstance := broker.NewInstance(brokerConfig)

				sigCh := make(chan os.Signal, 1)
				defer close(sigCh)
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				done := make(chan bool)
				defer close(done)

				go func() {
					defer func() {
						done <- true
					}()
					if err := brokerInstance.Start(); err != nil {
						log.Fatal(err)
					}
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
	startCmd.Flags().StringVarP(&configPath, "config-path", "i", constants.DefaultBrokerConfigPath, "broker config directory")
	startCmd.Flags().UintVar(&port, "port", constants.DefaultBrokerPort, "broker port")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().StringSliceVar(&zkQuorum, "zk-quorum", []string{"127.0.0.1:2181"}, "zookeeper quorum")
	startCmd.Flags().UintVar(&zkTimeout, "zk-timeout", 0, "zookeeper timeout")
	startCmd.Flags().StringVar(&hostname, "host", "127.0.0.1", "pirius broker hostname for registered bootstrapping")
	startCmd.Flags().StringVar(&bind, "bind", "127.0.0.1", "bind address")
	brokerConfig.BindPFlags(startCmd.Flags())
	brokerConfig.BindPFlag("zookeeper.quorum", startCmd.Flags().Lookup("zk-quorum"))
	brokerConfig.BindPFlag("zookeeper.timeout", startCmd.Flags().Lookup("zk-timeout"))

	return startCmd
}
