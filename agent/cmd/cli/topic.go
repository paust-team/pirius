package cli

import (
	"context"
	"errors"
	"fmt"
	"github.com/paust-team/pirius/bootstrapping/broker"
	"github.com/paust-team/pirius/coordinating/zk"
	"github.com/paust-team/pirius/proto/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

func NewTopicCmd() *cobra.Command {

	var topicCmd = &cobra.Command{
		Use:   "topic",
		Short: "topic commands",
	}

	topicCmd.PersistentFlags().StringSliceVar(&zkQuorum, "zk-quorum", []string{"127.0.0.1:2181"}, "zookeeper quorum")
	topicCmd.PersistentFlags().UintVar(&zkTimeout, "zk-timeout", 5000, "zookeeper timeout")

	topicCmd.AddCommand(
		NewCreateTopicCmd(),
		NewDeleteTopicCmd(),
	)

	return topicCmd
}

func newTopicClient() (pb.TopicClient, error) {
	coordClient := zk.NewZKCoordClient(zkQuorum, zkTimeout)
	if err := coordClient.Connect(); err != nil {
		return nil, err
	}
	defer coordClient.Close()

	brokers := broker.NewCoordClientWrapper(coordClient)

	brokerAddresses, err := brokers.GetBrokers()
	if err != nil {
		return nil, err
	}
	if len(brokerAddresses) == 0 {
		return nil, fmt.Errorf("not existed broker address in zk")
	}
	brokerAddr, err := brokers.GetBroker(brokerAddresses[0])
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(brokerAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	topicClient := pb.NewTopicClient(conn)
	return topicClient, nil
}

func NewCreateTopicCmd() *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use:   "create",
		Short: "Create topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			topicClient, err := newTopicClient()
			if err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
			defer func() {
				cancel()
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					fmt.Printf("topic client operation is timeout error")
				}
			}()

			topicOption := uint32(pb.TopicOption_NONE)
			if unique {
				topicOption = uint32(pb.TopicOption_UNIQUE_PER_FRAGMENT)
			}

			if _, err := topicClient.CreateTopic(ctx, &pb.CreateTopicRequest{
				Magic:       1,
				Name:        topic,
				Description: "",
				Options:     &topicOption,
			}); err != nil {
				return err
			}

			fmt.Printf("topic(%s) created\n", topic)
			return nil
		},
	}

	createTopicCmd.Flags().StringVarP(&topic, "topic", "t", "", "new topic name to create")
	createTopicCmd.Flags().BoolVarP(&unique, "unique", "u", false, "set topic as UniquePerFragment")

	createTopicCmd.MarkFlagRequired("topic")

	return createTopicCmd
}

func NewDeleteTopicCmd() *cobra.Command {

	var deleteTopicCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			topicClient, err := newTopicClient()
			if err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
			defer func() {
				cancel()
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					fmt.Printf("topic client operation is timeout error")
				}
			}()

			if _, err := topicClient.DeleteTopic(ctx, &pb.TopicRequestWithName{
				Magic: 1,
				Name:  topic,
			}); err != nil {
				return err
			}

			fmt.Printf("topic(%s) deleted\n", topic)
			return nil
		},
	}

	deleteTopicCmd.Flags().StringVarP(&topic, "topic", "t", "", "new topic name to create")
	deleteTopicCmd.MarkFlagRequired("topic")

	return deleteTopicCmd
}
