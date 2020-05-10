package cli

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/zookeeper"
	"github.com/spf13/cobra"
	"log"
	"os"
	"time"
)

var (
	topicName         string
	topicMeta         string
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

			rpcClient := client.NewRPCClient(zkAddr)
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

			rpcClient := client.NewRPCClient(zkAddr).WithTimeout(time.Duration(timeout) * time.Second)
			defer rpcClient.Close()

			if err := rpcClient.Connect(); err != nil {
				log.Fatal(err)
			}

			if err := rpcClient.DeleteTopic(ctx, topicName); err != nil {
				log.Fatal(err)
			}

			log.Println("delete topic ok")
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

			zkClient := zookeeper.NewZKClient(zkAddr)
			defer zkClient.Close()

			if zkClient.Connect() != nil {
				log.Fatal("cannot connect to zk")
			}

			topics, err := zkClient.GetTopics()
			if err != nil {
				log.Fatal(err)
			}

			for _, topic := range topics {
				log.Printf("%s, %s, %d, %d", topic, "", 0, 0)
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

			zkClient := zookeeper.NewZKClient(zkAddr)
			brokers, err := zkClient.GetTopicBrokers(topicName)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Topic: %s, Broker hosts: %s", topicName, brokers)
		},
	}

	describeTopicCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to describe")
	describeTopicCmd.MarkFlagRequired("topic")
	return describeTopicCmd
}
