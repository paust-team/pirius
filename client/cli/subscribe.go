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

	var subscribeCmd = &cobra.Command{
		Use:   "subscribe",
		Short: "subscribe data from topic",
		Run: func(cmd *cobra.Command, args []string) {
			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			consumerConfig := config.NewConsumerConfig()
			consumerConfig.Load(configDir)
			consumer := client.NewConsumer(consumerConfig, topicName)
			if err := consumer.Connect(); err != nil {
				log.Fatal(err)
			}

			defer consumer.Close()
			defer fmt.Println("done subscribe")

			subscribeCh, errCh, err := consumer.Subscribe(startOffset)
			if err != nil {
				log.Fatal(err)
			}

			for {
				select {
				case response := <-subscribeCh:
					fmt.Println("received topic data:", response.Data)
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
	subscribeCmd.Flags().StringVarP(&configDir, "config-dir", "c", common.DefaultConsumerConfigDir, "consumer client config directory")
	subscribeCmd.MarkFlagRequired("topic")

	return subscribeCmd
}
