package cli

import (
	"context"
	"github.com/paust-team/paustq/client/producer"
	"github.com/spf13/cobra"
	"log"
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
				log.Fatal("cannot connect to broker")
			}

			client.Publish(data)
			client.WaitAllPublishResponse()

			log.Println("published ok")
		},
	}

	publishCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to publish to")
	publishCmd.Flags().BytesBase64VarP(&data, "data", "d", nil, "base64 data")

	return publishCmd
}