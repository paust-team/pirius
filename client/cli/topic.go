package cli

import (
	"github.com/spf13/cobra"
)

var (
	topicName 			string
	numPartition 		uint8
	replicationFactor	uint8
)

func NewCreateTopicCmd() *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use: "create-topic",
		Short: "Create topic",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	createTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "","new topic name to create")
	createTopicCmd.Flags().Uint8VarP(&numPartition, "partitions", "p", 1, "num partition")
	createTopicCmd.Flags().Uint8VarP(&replicationFactor, "replication-factor", "r", 1, "replication factor")

	return createTopicCmd
}

func NewListTopicCmd() *cobra.Command {

	var listTopicCmd = &cobra.Command{
		Use: "list-topic",
		Short: "Get list of all existing topics",
		Run: func(cmd *cobra.Command, args []string) {
			// Todo:: send rpc request
		},
	}

	listTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "","topic name to listing")

	return listTopicCmd
}
