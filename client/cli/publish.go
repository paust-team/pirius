package cli

import (
	"bufio"
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var (
	filePath string
)

func NewPublishCmd() *cobra.Command {

	producerConfig := config.NewProducerConfig()

	var publishCmd = &cobra.Command{
		Use:   "publish (byte-string-data)",
		Short: "Publish data to topic",
		Run: func(cmd *cobra.Command, args []string) {
			sigCh := make(chan os.Signal, 1)

			producerConfig.Load(configPath)
			producer := client.NewProducer(producerConfig, topicName)

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
						fmt.Printf("publish succeed, partition id : %d, offset : %d\n", partition.Id, partition.Offset)
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
				fmt.Printf("publish succeed, partition id : %d, offset : %d\n", partition.Id, partition.Offset)
			} else {
				fmt.Println("no data to publish")
				os.Exit(1)
			}

			fmt.Println("done publish")
		},
	}

	publishCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to publish to")
	publishCmd.Flags().StringVarP(&filePath, "file-path", "f", "", "path of file to publish")
	publishCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultProducerConfigPath, "producer client config path")
	publishCmd.Flags().StringVar(&brokerHost, "broker-host", "", "broker host")
	publishCmd.Flags().UintVar(&brokerPort, "broker-port", 0, "broker port")
	publishCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	publishCmd.Flags().IntVar(&timeout, "timeout", 0, "connection timeout (seconds)")

	publishCmd.MarkFlagRequired("topic")

	producerConfig.BindPFlags(publishCmd.Flags())
	producerConfig.BindPFlag("broker.host", publishCmd.Flags().Lookup("broker-host"))
	producerConfig.BindPFlag("broker.port", publishCmd.Flags().Lookup("broker-port"))

	return publishCmd
}
