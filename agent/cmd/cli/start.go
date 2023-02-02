package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/pirius/agent"
	"github.com/paust-team/pirius/agent/config"
	"github.com/paust-team/pirius/agent/pubsub"
	"github.com/paust-team/pirius/constants"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	configPath string
	logDir     string
	dataDir    string
	logLevel   uint8
	port       uint
	zkQuorum   []string
	zkTimeout  uint
	topic      string
	unique     bool
)

func NewStartPublishCmd() *cobra.Command {

	agentConfig := config.NewAgentConfig()
	var startCmd = &cobra.Command{
		Use:   "start-publish",
		Short: "start pirius agent for publishing",
		Run: func(cmd *cobra.Command, args []string) {
			publisher := agent.NewRetrievablePubSubAgent(agentConfig)

			if err := publisher.StartWithServer(); err != nil {
				log.Fatal(err)
			}

			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			done := make(chan bool)
			defer close(done)
			sendCh := make(chan pubsub.TopicData)
			defer close(sendCh)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			retrieveCh, err := publisher.StartRetrievablePublish(ctx, topic, sendCh)
			if err != nil {
				panic(err)
			}

			go func() {
				defer func() {
					done <- true
				}()
				for result := range retrieveCh {
					fmt.Printf("retrieved result = %+v\n", result)
				}
			}()

			counter := 0
			for {
				select {
				case <-done:
					fmt.Println("agent process finished")
					return
				case sig := <-sigCh:
					fmt.Println("received signal:", sig)
					publisher.Stop()
				case <-time.After(time.Second):
					sendCh <- pubsub.TopicData{
						SeqNum: uint64(counter),
						Data:   []byte(fmt.Sprintf("record %d", counter)),
					}
					counter++
				}
			}
		},
	}

	startCmd.Flags().StringVarP(&topic, "topic", "t", "test", "topic name")
	startCmd.Flags().StringVarP(&configPath, "config-path", "i", constants.DefaultAgentConfigPath, "agent config directory")
	startCmd.Flags().UintVar(&port, "port", constants.DefaultAgentPort, "agent port")
	startCmd.Flags().StringVar(&logDir, "log-dir", "", "log directory")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "", "data directory")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().StringSliceVar(&zkQuorum, "zk-quorum", []string{"127.0.0.1:2181"}, "zookeeper quorum")
	startCmd.Flags().UintVar(&zkTimeout, "zk-timeout", 5000, "zookeeper timeout")

	agentConfig.BindPFlags(startCmd.Flags())
	agentConfig.BindPFlag("zookeeper.quorum", startCmd.Flags().Lookup("zk-quorum"))
	agentConfig.BindPFlag("zookeeper.timeout", startCmd.Flags().Lookup("zk-timeout"))

	return startCmd
}

func NewStartSubscribeCmd() *cobra.Command {

	agentConfig := config.NewAgentConfig()
	var startCmd = &cobra.Command{
		Use:   "start-subscribe",
		Short: "start pirius agent for subscribing",
		Run: func(cmd *cobra.Command, args []string) {
			subscriber := agent.NewRetrievablePubSubAgent(agentConfig)

			if err := subscriber.Start(); err != nil {
				log.Fatal(err)
			}

			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			done := make(chan bool)
			defer close(done)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			recvCh, err := subscriber.StartRetrievableSubscribe(ctx, topic, 1, 0)
			if err != nil {
				panic(err)
			}

			go func() {
				defer func() {
					done <- true
				}()
				retrievePostfix := []byte("_processed")
				for result := range recvCh {
					fmt.Printf("recvCh result = %+v\n", result)
					results := []pubsub.SubscriptionResult{{
						FragmentId: result.Results[0].FragmentId,
						SeqNum:     result.Results[0].SeqNum,
						Data:       append(result.Results[0].Data, retrievePostfix...),
					}}
					if err := result.SendBack(results); err != nil {
						panic(err)
					}
				}
			}()

			for {
				select {
				case <-done:
					fmt.Println("agent process finished")
					return
				case sig := <-sigCh:
					fmt.Println("received signal:", sig)
					subscriber.Stop()
				}
			}
		},
	}

	startCmd.Flags().StringVarP(&topic, "topic", "t", "test", "topic name")
	startCmd.Flags().StringVarP(&configPath, "config-path", "i", constants.DefaultAgentConfigPath, "agent config directory")
	startCmd.Flags().StringVar(&logDir, "log-dir", "", "log directory")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "", "data directory")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().StringSliceVar(&zkQuorum, "zk-quorum", []string{"127.0.0.1:2181"}, "zookeeper quorum")
	startCmd.Flags().UintVar(&zkTimeout, "zk-timeout", 5000, "zookeeper timeout")

	agentConfig.BindPFlags(startCmd.Flags())
	agentConfig.BindPFlag("zookeeper.quorum", startCmd.Flags().Lookup("zk-quorum"))
	agentConfig.BindPFlag("zookeeper.timeout", startCmd.Flags().Lookup("zk-timeout"))

	return startCmd
}
