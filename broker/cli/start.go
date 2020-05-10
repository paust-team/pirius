package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/broker"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

var (
	logDir   string
	dataDir  string
	logLevel uint8
	port     uint16
	zkAddr   string
)

func NewStartCmd() *cobra.Command {

	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start paustq broker",
		Run: func(cmd *cobra.Command, args []string) {
			brokerInstance := broker.NewBroker(zkAddr)

			if cmd.Flags().Changed("port") {
				brokerInstance = brokerInstance.WithPort(port)
			}
			if cmd.Flags().Changed("log-dir") {
				brokerInstance = brokerInstance.WithLogDir(logDir)
			}
			if cmd.Flags().Changed("data-dir") {
				brokerInstance = brokerInstance.WithDataDir(dataDir)
			}
			if cmd.Flags().Changed("log-level") {
				brokerInstance = brokerInstance.WithLogLevel(logger.LogLevel(logLevel))
			}

			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			brokerCtx, cancel := context.WithCancel(context.Background())
			done := make(chan bool)
			defer close(done)

			go func() {
				if err := brokerInstance.Start(brokerCtx); err != nil {
					done <- true
					fmt.Println(err)
				}
				done <- true
			}()

			for {
				select {
				case <-done:
					fmt.Println("broker process finished")
					return
				case sig := <-sigCh:
					fmt.Println("received signal:", sig)
					cancel()
				}
			}
		},
	}

	startCmd.Flags().StringVar(&logDir, "log-dir", broker.DefaultLogDir, "log directory")
	startCmd.Flags().StringVar(&dataDir, "data-dir", broker.DefaultDataDir, "data directory")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", uint8(broker.DefaultLogLevel), "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().Uint16Var(&port, "port", common.DefaultBrokerPort, "broker port")
	startCmd.Flags().StringVarP(&zkAddr, "zk-addr", "z", "", "zookeeper ip address")

	startCmd.MarkFlagRequired("zk-addr")

	return startCmd
}
