package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/client/consumer"
	"github.com/spf13/cobra"
	"log"
	"os"
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
				log.Fatal("Cannot connect to broker")
				os.Exit(1)
			}

			for response := range client.Subscribe(startOffset) {
				if response.Error != nil {
					log.Fatal(response.Error)
				} else {
					fmt.Println("Received topic data: ", response.Data)
				}
			}
		},
	}

	subscribeCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to subscribe from")
	subscribeCmd.Flags().Uint64VarP(&startOffset, "offset", "o", 0,"start offset")

	return subscribeCmd
}