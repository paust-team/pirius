package cli

import (
	"fmt"
	client "github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	startOffset uint64
)

func NewSubscribeCmd() *cobra.Command {

	consumerConfig := config.NewConsumerConfig()

	var subscribeCmd = &cobra.Command{
		Use:   "subscribe",
		Short: "subscribe data from topic",
		Run: func(cmd *cobra.Command, args []string) {
			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			consumerConfig.Load(configPath)
			consumer := client.NewConsumer(consumerConfig, topicName)
			if err := consumer.Connect(); err != nil {
				log.Fatal(err)
			}

			defer consumer.Close()
			defer fmt.Println("done subscribe")

			subscribeCh, errCh, err := consumer.Subscribe(startOffset, 1, 0)
			if err != nil {
				log.Fatal(err)
			}

			for {
				select {
				case response := <-subscribeCh:
					fmt.Println("received topic data:", response.Items[0].Data)
				case sig := <-sigCh:
					fmt.Println("received signal:", sig)
					return
				case err := <-errCh:
					log.Fatal(err)
				}
			}
		},
	}

	subscribeCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to subscribe from")
	subscribeCmd.Flags().Uint64VarP(&startOffset, "offset", "o", 0, "start offset")
	subscribeCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultConsumerConfigPath, "consumer client config path")
	subscribeCmd.Flags().StringVar(&brokerHost, "broker-host", "", "broker host")
	subscribeCmd.Flags().UintVar(&brokerPort, "broker-port", 0, "broker port")
	subscribeCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	subscribeCmd.Flags().IntVar(&timeout, "timeout", 0, "connection timeout (seconds)")

	subscribeCmd.MarkFlagRequired("topic")

	consumerConfig.BindPFlags(subscribeCmd.Flags())
	consumerConfig.BindPFlag("broker.host", subscribeCmd.Flags().Lookup("broker-host"))
	consumerConfig.BindPFlag("broker.port", subscribeCmd.Flags().Lookup("broker-port"))

	return subscribeCmd
}
