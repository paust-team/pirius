package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/client/producer"
	"github.com/spf13/cobra"
	"log"
	"os"
	"time"
)

var (
	data 				[]byte
)

func NewPublishCmd() *cobra.Command {

	var publishCmd = &cobra.Command{
		Use: "publish",
		Short: "Publish data to topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			client := producer.NewProducer(ctx, bootstrapServer, time.Duration(timeout))
			defer client.Close()

			if client.Connect(topicName) != nil {
				log.Fatal("Cannot connect to broker")
				os.Exit(1)
			}

			client.Publish(data)
			client.WaitAllPublishResponse()

			fmt.Println("Publish done")
		},
	}

	publishCmd.Flags().StringVarP(&topicName, "topic", "tn", "","topic name to publish to")
	publishCmd.Flags().BytesBase64VarP(&data, "data", "d", nil, "base64 data")

	return publishCmd
}