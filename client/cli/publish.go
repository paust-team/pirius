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
			nodeId := common.GenerateNodeId()

			if cmd.Flags().Changed("file-path") {
				f, err := os.Open(filePath)
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()

				publishCh := make(chan *client.PublishData)
				fragmentCh, errCh, err := producer.AsyncPublish(publishCh)
				if err != nil {
					log.Fatal(err)
				}
				defer close(publishCh)
				scanner := bufio.NewScanner(f)

				seq := 0
				for scanner.Scan() {
					select {
					case publishCh <- &client.PublishData{Data: []byte(scanner.Text()), NodeId: nodeId, SeqNum: uint64(seq)}:
					case fragment := <-fragmentCh:
						fmt.Printf("publish succeed, fragment id : %d, offset : %d\n", fragment.Id, fragment.LastOffset)
					case err := <-errCh:
						log.Fatal(err)
					case sig := <-sigCh:
						fmt.Println("received signal:", sig)
						return
					}
					seq += 1
				}
			} else if len(args) > 0 {
				fragment, err := producer.Publish(&client.PublishData{Data: []byte(args[0]), NodeId: nodeId, SeqNum: 0})
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("publish succeed, fragment id : %d, last offset : %d\n", fragment.Id, fragment.LastOffset)
			} else {
				fmt.Println("no data to publish")
				os.Exit(1)
			}

			fmt.Println("done publish")
		},
	}

	publishCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to publish")
	publishCmd.Flags().Uint32VarP(&fragmentId, "fragment", "r", 0, "fragment id to publish")
	publishCmd.Flags().StringVarP(&filePath, "file-path", "f", "", "path of file to publish")
	publishCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultProducerConfigPath, "producer client config path")
	publishCmd.Flags().StringVar(&bootstrapServers, "bootstrap-servers", "", "bootstrap server addresses to connect (ex. localhost:2181)")
	publishCmd.Flags().UintVar(&bootstrapTimeout, "bootstrap-timeout", 0, "timeout for bootstrapping")
	publishCmd.Flags().IntVar(&timeout, "broker-timeout", 0, "connection timeout (milliseconds)")
	publishCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")

	publishCmd.MarkFlagRequired("topic")

	producerConfig.BindPFlags(publishCmd.Flags())
	producerConfig.BindPFlag("bootstrap.servers", publishCmd.Flags().Lookup("bootstrap-servers"))
	producerConfig.BindPFlag("bootstrap.timeout", publishCmd.Flags().Lookup("bootstrap-timeout"))

	return publishCmd
}
