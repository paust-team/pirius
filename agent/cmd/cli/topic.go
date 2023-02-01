package cli

import (
	"fmt"
	topic2 "github.com/paust-team/pirius/bootstrapping/topic"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/coordinating/zk"
	"github.com/spf13/cobra"
)

func NewTopicCmd() *cobra.Command {

	var topicCmd = &cobra.Command{
		Use:   "topic",
		Short: "topic commands",
	}

	topicCmd.PersistentFlags().StringSliceVar(&zkQuorum, "zk-quorum", []string{"127.0.0.1:2181"}, "zookeeper quorum")
	topicCmd.PersistentFlags().UintVar(&zkTimeout, "zk-timeout", 5000, "zookeeper timeout")

	coordClient := zk.NewZKCoordClient(zkQuorum, zkTimeout)

	topicCmd.AddCommand(
		NewCreateTopicCmd(coordClient),
		NewDeleteTopicCmd(coordClient),
	)

	return topicCmd
}

func NewCreateTopicCmd(coordClient coordinating.CoordClient) *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use:   "create",
		Short: "Create topic",
		Run: func(cmd *cobra.Command, args []string) {
			if err := coordClient.Connect(); err != nil {
				panic(err)
			}
			defer coordClient.Close()

			topicClient := topic2.NewCoordClientTopicWrapper(coordClient)
			var option topic2.Option
			if unique {
				option = topic2.UniquePerFragment
			}
			if err := topicClient.CreateTopic(topic, topic2.NewTopicFrame("", option)); err != nil {
				panic(err)
			}

			fmt.Printf("topic(%s) created\n", topic)
		},
	}

	createTopicCmd.Flags().StringVarP(&topic, "topic", "t", "", "new topic name to create")
	createTopicCmd.Flags().BoolVarP(&unique, "unique", "u", false, "set topic as UniquePerFragment")

	createTopicCmd.MarkFlagRequired("topic")

	return createTopicCmd
}

func NewDeleteTopicCmd(coordClient coordinating.CoordClient) *cobra.Command {

	var deleteTopicCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete topic",
		Run: func(cmd *cobra.Command, args []string) {
			if err := coordClient.Connect(); err != nil {
				panic(err)
			}
			defer coordClient.Close()

			topicClient := topic2.NewCoordClientTopicWrapper(coordClient)
			if err := topicClient.DeleteTopic(topic); err != nil {
				panic(err)
			}

			fmt.Printf("topic(%s) deleted\n", topic)
		},
	}

	deleteTopicCmd.Flags().StringVarP(&topic, "topic", "t", "", "new topic name to create")
	deleteTopicCmd.MarkFlagRequired("topic")

	return deleteTopicCmd
}
