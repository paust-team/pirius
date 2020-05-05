package cli

import (
	"context"
	"errors"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/zookeeper"
	"github.com/spf13/cobra"
	"log"
	"math/rand"
	"time"
)

var (
	topicName         string
	topicMeta         string
	numPartition      uint32
	replicationFactor uint32
)

func NewCreateTopicCmd() *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use:   "create-topic",
		Short: "Create topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			zkClient := zookeeper.NewZKClient(zkHost)
			var brokerHost string
			brokerHosts, err := zkClient.GetTopicBrokers(topicName)
			if err != nil {
				log.Fatal(err)
			}

			if brokerHosts == nil {
				brokers, err := zkClient.GetBrokers()
				if err != nil {
					log.Fatal(err)
				}
				if brokers == nil {
					log.Fatal(errors.New("broker doesn't exists"))
				}
				randBrokerIndex := rand.Intn(len(brokers))
				brokerHost = brokers[randBrokerIndex]
			} else {
				brokerHost = brokerHosts[0]
			}

			apiClient := client.NewAPIClient(brokerHost)
			defer apiClient.Close()

			if apiClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}

			if err := apiClient.CreateTopic(ctx, topicName, topicMeta, numPartition, replicationFactor); err != nil {
				log.Fatal(err)
			}

			log.Println("create topic ok")
		},
	}

	createTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "", "new topic name to create")
	createTopicCmd.MarkFlagRequired("topic")
	createTopicCmd.Flags().StringVarP(&topicMeta, "topic-meta", "m", "", "topic meta for topic")
	createTopicCmd.Flags().Uint32VarP(&numPartition, "partitions", "p", 1, "num partition")
	createTopicCmd.Flags().Uint32VarP(&replicationFactor, "replication-factor", "r", 1, "replication factor")

	return createTopicCmd
}

func NewDeleteTopicCmd() *cobra.Command {

	var deleteTopicCmd = &cobra.Command{
		Use:   "delete-topic",
		Short: "Delete topic",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			zkClient := zookeeper.NewZKClient(zkHost)
			brokers, err := zkClient.GetTopicBrokers(topicName)
			if err != nil {
				log.Fatal(err)
			}
			apiClient := client.NewAPIClient(brokers[0]).WithTimeout(time.Duration(timeout) * time.Second)
			defer apiClient.Close()

			if apiClient.Connect() != nil {
				log.Fatal("cannot connect to broker")
			}

			if err := apiClient.DeleteTopic(ctx, topicName); err != nil {
				log.Fatal(err)
			}

			log.Println("delete topic ok")
		},
	}

	deleteTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "", "topic name to delete")

	return deleteTopicCmd
}

func NewListTopicCmd() *cobra.Command {

	var listTopicCmd = &cobra.Command{
		Use:   "list-topic",
		Short: "Get list of all existing topics",
		Run: func(cmd *cobra.Command, args []string) {

			zkClient := zookeeper.NewZKClient(zkHost)
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

	listTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "", "topic name to listing")

	return listTopicCmd
}

func NewDescribeTopicCmd() *cobra.Command {

	var describeTopicCmd = &cobra.Command{
		Use:   "describe-topic",
		Short: "Describe topic",
		Run: func(cmd *cobra.Command, args []string) {

			zkClient := zookeeper.NewZKClient(zkHost)
			brokers, err := zkClient.GetTopicBrokers(topicName)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Topic: %s, Broker hosts: %s", topicName, brokers)
		},
	}

	describeTopicCmd.Flags().StringVarP(&topicName, "topic", "c", "", "topic name to describe")

	return describeTopicCmd
}
