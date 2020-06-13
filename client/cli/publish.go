package cli

import (
	"bufio"
	"fmt"
	"github.com/paust-team/paustq/client"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var (
	filePath string
)

func NewPublishCmd() *cobra.Command {
	var publishCmd = &cobra.Command{
		Use:   "publish (byte-string-data)",
		Short: "Publish data to topic",
		Run: func(cmd *cobra.Command, args []string) {
			sigCh := make(chan os.Signal, 1)
			producer := client.NewProducer(brokerAddr, topicName).WithTimeout(timeout).WithLogLevel(logger.Error)

			if err := producer.Connect(); err != nil {
				log.Fatal(err)
			}

			defer producer.Close()

			if cmd.Flags().Changed("file-path") {
				f, err := os.Open(filePath)
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()

				publishCh := make(chan []byte)
				partitionCh, errCh, err := producer.AsyncPublish(publishCh)
				if err != nil {
					log.Fatal(err)
				}
				defer close(publishCh)
				scanner := bufio.NewScanner(f)

				for scanner.Scan() {
					select {
					case publishCh <- []byte(scanner.Text()):
					case partition := <-partitionCh:
						fmt.Printf("publish succeed, partition id : %d, offset : %d", partition.Id, partition.Offset)
					case err := <-errCh:
						log.Fatal(err)
					case sig := <-sigCh:
						fmt.Println("received signal:", sig)
						return
					}
				}
			} else if len(args) > 0 {
				partition, err := producer.Publish([]byte(args[0]))
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("publish succeed, partition id : %d, offset : %d", partition.Id, partition.Offset)
			} else {
				fmt.Println("no data to publish")
				os.Exit(1)
			}

			fmt.Println("done publish")
		},
	}

	publishCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to publish to")
	publishCmd.Flags().StringVarP(&filePath, "file-path", "f", "", "path of file to publish")

	publishCmd.MarkFlagRequired("topic")

	return publishCmd
}
