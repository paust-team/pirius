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

			consumerConfig.Load(consumerConfigPath)
			topicFragments := common.NewTopicFromFragmentOffsets(topicName, common.FragmentOffsetMap{fragmentId: startOffset}, 1, 0)
			consumer := client.NewConsumer(consumerConfig, []*common.Topic{topicFragments})
			if err := consumer.Connect(); err != nil {
				log.Fatal(err)
			}

			defer consumer.Close()
			defer fmt.Println("done subscribe")

			subscribeCh, errCh, err := consumer.Subscribe()
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

	flags := subscribeCmd.Flags()
	flags.StringVarP(&topicName, "topic", "n", "", "topic name to subscribe from")
	flags.Uint64VarP(&startOffset, "offset", "o", 0, "start offset")
	flags.StringVarP(&consumerConfigPath, "config-path", "i", common.DefaultConsumerConfigPath, "consumer client config path")
	flags.StringVar(&bootstrapServers, "bootstrap-servers", "", "bootstrap server addresses to connect (ex. localhost:2181)")
	flags.UintVar(&bootstrapTimeout, "bootstrap-timeout", 0, "timeout for bootstrapping")
	flags.IntVar(&timeout, "broker-timeout", 0, "connection timeout (milliseconds)")
	flags.Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	flags.Uint32VarP(&fragmentId, "fragment", "f", 0, "fragment id")

	subscribeCmd.MarkFlagRequired("topic")

	consumerConfig.BindPFlags(flags)
	consumerConfig.BindPFlag("bootstrap.servers", flags.Lookup("bootstrap-servers"))
	consumerConfig.BindPFlag("bootstrap.timeout", flags.Lookup("bootstrap-timeout"))

	return subscribeCmd
}
