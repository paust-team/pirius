package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/client/consumer"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

var (
	startOffset uint64
)

func NewSubscribeCmd() *cobra.Command {

	var subscribeCmd = &cobra.Command{
		Use:   "subscribe",
		Short: "subscribe data from topic",
		Run: func(cmd *cobra.Command, args []string) {

			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			ctx := context.Background()
			client := consumer.NewConsumer(brokerAddr).WithLogLevel(logger.Error)

			if err := client.Connect(ctx, topicName); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer client.Close()
			defer fmt.Println("done subscribe")

			subscribeCh, errCh := client.Subscribe(ctx, startOffset)
			go func() {
				select {
				case err := <-errCh:
					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
					return
				case <-ctx.Done():
					return
				}
			}()

			for {
				select {
				case response:= <- subscribeCh:
					fmt.Println("received topic data:", response.Data)
				case sig := <-sigCh:
					fmt.Println("received signal:", sig)
					return
				}
			}
		},
	}

	subscribeCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to subscribe from")
	subscribeCmd.Flags().Uint64VarP(&startOffset, "offset", "o", 0, "start offset")

	subscribeCmd.MarkFlagRequired("topic")

	return subscribeCmd
}
