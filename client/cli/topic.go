package cli

import (
	"context"
	"github.com/paust-team/paustq/client"
	"github.com/spf13/cobra"
	"log"
)

var (
	topicName 			string
	topicMeta 			string
	numPartition 		uint32
	replicationFactor	uint32
)

func NewCreateTopicCmd() *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use: "create-topic",
		Short: "Create topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			topicRpcClient := client.NewTopicServiceRpcClient(ctx, bootstrapServer)
			defer topicRpcClient.Close()

			if topicRpcClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}

			if err := topicRpcClient.CreateTopic(topicName, topicMeta, numPartition, replicationFactor); err != nil {
				log.Fatal(err)
			}

			log.Println("create topic ok")
		},
	}

	createTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "","new topic name to create")
	createTopicCmd.Flags().StringVarP(&topicMeta, "topic-meta", "m", "","topic meta for topic")
	createTopicCmd.Flags().Uint32VarP(&numPartition, "partitions", "p", 1, "num partition")
	createTopicCmd.Flags().Uint32VarP(&replicationFactor, "replication-factor", "r", 1, "replication factor")

	return createTopicCmd
}

func NewListTopicCmd() *cobra.Command {

	var listTopicCmd = &cobra.Command{
		Use: "list-topic",
		Short: "Get list of all existing topics",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			topicRpcClient := client.NewTopicServiceRpcClient(ctx, bootstrapServer)
			defer topicRpcClient.Close()

			if topicRpcClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}
			topics, err := topicRpcClient.ListTopics()
			if err != nil {
				log.Fatal(err)
			}

			for _, topic := range topics.Topics {
				log.Printf("%s, %s, %d, %d", topic.TopicName, topic.TopicMeta, topic.NumPartitions, topic.ReplicationFactor)
			}
		},
	}

	listTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to listing")

	return listTopicCmd
}

func NewDeleteTopicCmd() *cobra.Command {

	var deleteTopicCmd = &cobra.Command{
		Use: "delete-topic",
		Short: "Delete topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			topicRpcClient := client.NewTopicServiceRpcClient(ctx, bootstrapServer)
			defer topicRpcClient.Close()

			if topicRpcClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}

			if err := topicRpcClient.DeleteTopic(topicName); err != nil {
				log.Fatal(err)
			}

			log.Println("delete topic ok")
		},
	}

	deleteTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to delete")

	return deleteTopicCmd
}

func NewDescribeTopicCmd() *cobra.Command {

	var describeTopicCmd = &cobra.Command{
		Use: "describe-topic",
		Short: "Describe topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			topicRpcClient := client.NewTopicServiceRpcClient(ctx, bootstrapServer)
			defer topicRpcClient.Close()

			if topicRpcClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}
			resp, err := topicRpcClient.DescribeTopic(topicName);
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Topic: %s, NumPublisher: %d, NumSubscriber: %d", resp.Topic, resp.NumPublishers, resp.NumSubscribers)
		},
	}

	describeTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to describe")

	return describeTopicCmd
}