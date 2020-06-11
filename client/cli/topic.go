package cli

import (
	"fmt"
	"github.com/paust-team/paustq/client"
	logger "github.com/paust-team/paustq/log"
	"github.com/spf13/cobra"
	"os"
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
			adminClient := client.NewAdminClient(brokerAddr).WithLogLevel(logger.Error)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			if err := adminClient.CreateTopic(topicName, topicMeta, 1, 1); err != nil {
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
			adminClient := client.NewAdminClient(brokerAddr).WithTimeout(timeout).WithLogLevel(logger.Error)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			if err := adminClient.DeleteTopic(topicName); err != nil {
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

			adminClient := client.NewAdminClient(brokerAddr).WithTimeout(timeout).WithLogLevel(logger.Error)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			response, err := adminClient.ListTopic()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			for _, topic := range response.Topics {
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

			adminClient := client.NewAdminClient(brokerAddr).WithTimeout(timeout).WithLogLevel(logger.Error)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			response, err := adminClient.DescribeTopic(topicName)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Printf("Topic: %s, Topic meta: %s, Num partitions: %d, replication factor: %d", response.Topic.TopicName,
				response.Topic.TopicMeta, response.Topic.NumPartitions, response.Topic.ReplicationFactor)
		},
	}

	describeTopicCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to describe")
	describeTopicCmd.MarkFlagRequired("topic")
	return describeTopicCmd
}
