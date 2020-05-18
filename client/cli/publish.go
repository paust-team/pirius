package cli

import (
	"bufio"
	"context"
	"fmt"
	"github.com/paust-team/paustq/client/producer"
	"github.com/paust-team/paustq/common"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"log"
	"os"
	"time"
)

var (
	chunkSize uint32
	filePath  string
)

func NewPublishCmd() *cobra.Command {

	var publishCmd = &cobra.Command{
		Use:   "publish (byte-string-data)",
		Short: "Publish data to topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			client := producer.NewProducer(zkAddr).WithTimeout(time.Duration(timeout)).WithLogLevel(logger.Error)

			if cmd.Flags().Changed("chunk") {
				client = client.WithChunkSize(chunkSize)
			}

			if err := client.Connect(ctx, topicName); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer client.Close()

			if cmd.Flags().Changed("file-path") {

				f, err := os.Open(filePath)
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()

				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					client.Publish(ctx, []byte(scanner.Text()))
				}

			} else if len(args) > 0 {
				client.Publish(ctx, []byte(args[0]))
			} else {
				fmt.Println("no data to publish")
				os.Exit(1)
			}

			client.WaitAllPublishResponse()

			fmt.Println("done publish")
		},
	}

	publishCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to publish to")
	publishCmd.Flags().Uint32VarP(&chunkSize, "chunk", "c", common.DefaultChunkSize, "chunk size")
	publishCmd.Flags().StringVarP(&filePath, "file-path", "f", "", "path of file to publish")

	publishCmd.MarkFlagRequired("topic")

	return publishCmd
}
