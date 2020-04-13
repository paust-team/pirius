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
			// Todo:: send rpc request
		},
	}

	createTopicCmd.Flags().StringVarP(&topicName, "name", "n", "","new topic name to create")
	createTopicCmd.Flags().Uint8VarP(&numPartition, "partitions", "pt", 1, "num partition")
	createTopicCmd.Flags().Uint8VarP(&replicationFactor, "replication-factor", "rf", 1, "replication factor")

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

	listTopicCmd.Flags().StringVarP(&topicName, "topic", "tn", "","new topic name to create")

	return listTopicCmd
}
