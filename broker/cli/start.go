package cli

import (
	"context"
	"github.com/paust-team/paustq/broker"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"log"
)

var (
	logDir    	string
	dataDir    	string
	logLevel	uint8
	port   		uint16
	zkAddr 		string
)

func NewStartCmd() *cobra.Command {

	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start paustq broker",
		Run: func(cmd *cobra.Command, args []string) {
			brokerInstance := broker.NewBroker(zkAddr)

			if port != common.DefaultBrokerPort {
				brokerInstance = brokerInstance.WithPort(port)
			}
			if logDir != broker.DefaultLogDir {
				brokerInstance = brokerInstance.WithLogDir(logDir)
			}
			if dataDir != broker.DefaultDataDir {
				brokerInstance = brokerInstance.WithDataDir(dataDir)
			}
			lv := logger.LogLevel(logLevel)
			if lv != broker.DefaultLogLevel {
				brokerInstance = brokerInstance.WithLogLevel(lv)
			}

			if err := brokerInstance.Start(context.Background()); err != nil {
				log.Fatal(err)
			}
		},
	}

	startCmd.Flags().StringVar(&logDir, "log-dir", broker.DefaultLogDir, "log directory")
	startCmd.Flags().StringVar(&dataDir, "data-dir", broker.DefaultDataDir, "data directory")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", uint8(broker.DefaultLogLevel), "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().Uint16Var(&port, "port", common.DefaultBrokerPort, "broker port")
	startCmd.Flags().StringVarP(&zkAddr, "zk-addr", "z", "127.0.0.1", "zookeeper ip address")

	startCmd.MarkFlagRequired("zk-addr")

	return startCmd
}
