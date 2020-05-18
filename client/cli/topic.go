package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/client"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/zookeeper"
	"github.com/spf13/cobra"
	"os"
	"time"
)

var (
	topicName string
	topicMeta string
)

func NewTopicCmd() *cobra.Command {

	var topicCmd = &cobra.Command{
		Use:   "topic",
		Short: "topic commands",
	}

	topicCmd.AddCommand(
		NewCreateTopicCmd(),
		NewListTopicCmd(),
		NewDeleteTopicCmd(),
		NewDescribeTopicCmd(),
	)

	return topicCmd
}

func NewCreateTopicCmd() *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use:   "create",
		Short: "Create topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()

			rpcClient := client.NewRPCClient(zkAddr).WithLogLevel(logger.Error)
			defer rpcClient.Close()

			if err := rpcClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			if err := rpcClient.CreateTopic(ctx, topicName, topicMeta, 1, 1); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Println("create topic ok")
		},
	}

	createTopicCmd.Flags().StringVarP(&topicName, "topic", "n", "", "new topic name to create")
	createTopicCmd.Flags().StringVarP(&topicMeta, "topic-meta", "m", "", "topic meta for topic")
	createTopicCmd.MarkFlagRequired("topic")

	return createTopicCmd
}

func NewDeleteTopicCmd() *cobra.Command {

	var deleteTopicCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()

			rpcClient := client.NewRPCClient(zkAddr).WithTimeout(time.Duration(timeout) * time.Second).WithLogLevel(logger.Error)
			defer rpcClient.Close()

			if err := rpcClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			if err := rpcClient.DeleteTopic(ctx, topicName); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Printf("topic %s deleted", topicName)
		},
	}

	deleteTopicCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to delete")
	deleteTopicCmd.MarkFlagRequired("topic")
	return deleteTopicCmd
}

func NewListTopicCmd() *cobra.Command {

	var listTopicCmd = &cobra.Command{
		Use:   "list",
		Short: "Get list of all existing topics",
		Run: func(cmd *cobra.Command, args []string) {

			zkClient := zookeeper.NewZKClient(zkAddr).WithLogger(defaultLogger)
			defer zkClient.Close()

			if err := zkClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			topics, err := zkClient.GetTopics()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			for _, topic := range topics {
				fmt.Printf("%s, %s", topic, "")
			}
		},
	}

	return listTopicCmd
}

func NewDescribeTopicCmd() *cobra.Command {

	var describeTopicCmd = &cobra.Command{
		Use:   "describe",
		Short: "Describe topic",
		Run: func(cmd *cobra.Command, args []string) {

			zkClient := zookeeper.NewZKClient(zkAddr).WithLogger(defaultLogger)
			brokers, err := zkClient.GetTopicBrokers(topicName)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Printf("Topic: %s, Broker hosts: %s", topicName, brokers)
		},
	}

	describeTopicCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to describe")
	describeTopicCmd.MarkFlagRequired("topic")
	return describeTopicCmd
}
