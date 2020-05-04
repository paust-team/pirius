package cli

import (
	"context"
	"github.com/paust-team/paustq/client/consumer"
	"github.com/spf13/cobra"
	"log"
)

var (
	startOffset uint64
)

func NewSubscribeCmd() *cobra.Command {

	var subscribeCmd = &cobra.Command{
		Use:   "subscribe",
		Short: "subscribe data from topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			client := consumer.NewConsumer(bootstrapServer)
			defer client.Close()

			if client.Connect(ctx, topicName) != nil {
				log.Fatal("cannot connect to broker")
			}
			subscribeChan, err := client.Subscribe(ctx, startOffset)
			if err != nil {
				log.Fatal(err)
			}
			for response := range subscribeChan {
				if response.Error != nil {
					log.Fatal(response.Error)
				} else {
					log.Println("received topic data: ", response.Data)
				}
			}
		},
	}

	subscribeCmd.Flags().StringVarP(&topicName, "topic", "c", "", "topic name to subscribe from")
	subscribeCmd.MarkFlagRequired("topic")
	subscribeCmd.Flags().Uint64VarP(&startOffset, "offset", "o", 0, "start offset")

	return subscribeCmd
}
