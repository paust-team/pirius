package cli

import (
	"context"
	"github.com/paust-team/paustq/client/consumer"
	"github.com/spf13/cobra"
	"log"
	"time"
)

var (
	startOffset 				uint64
)

func NewSubscribeCmd() *cobra.Command {

	var subscribeCmd = &cobra.Command{
		Use: "subscribe",
		Short: "subscribe data from topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			client := consumer.NewConsumer(ctx, bootstrapServer, time.Duration(timeout))
			defer client.Close()

			if client.Connect(topicName) != nil {
				log.Fatal("cannot connect to broker")
			}

			for response := range client.Subscribe(startOffset) {
				if response.Error != nil {
					log.Fatal(response.Error)
				} else {
					log.Println("received topic data: ", response.Data)
				}
			}
		},
	}

	subscribeCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to subscribe from")
	subscribeCmd.MarkFlagRequired("topic")
	subscribeCmd.Flags().Uint64VarP(&startOffset, "offset", "o", 0,"start offset")

	return subscribeCmd
}