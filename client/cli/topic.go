package cli

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	"github.com/spf13/cobra"
	"os"
)

func NewTopicCmd() *cobra.Command {

	var topicCmd = &cobra.Command{
		Use:   "topic",
		Short: "topic commands",
	}

	topicCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultAdminConfigPath, "admin client config path")
	topicCmd.Flags().StringVar(&bootstrapServers, "broker-address", "", "broker address to connect (ex. localhost:1101)")
	topicCmd.Flags().IntVar(&timeout, "broker-timeout", 0, "connection timeout (milliseconds)")
	topicCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")

	adminConfig := config.NewAdminConfig()

	adminConfig.BindPFlags(topicCmd.Flags())
	adminConfig.BindPFlag("bootstrap.servers", topicCmd.Flags().Lookup("broker-address"))

	topicCmd.AddCommand(
		NewCreateTopicCmd(adminConfig),
		NewListTopicCmd(adminConfig),
		NewDeleteTopicCmd(adminConfig),
		NewDescribeTopicCmd(adminConfig),
	)

	return topicCmd
}

func NewCreateTopicCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var createTopicCmd = &cobra.Command{
		Use:   "create",
		Short: "Create topic",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(configPath)
			adminClient := client.NewAdmin(adminConfig)
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

func NewDeleteTopicCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var deleteTopicCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete topic",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(configPath)
			adminClient := client.NewAdmin(adminConfig)
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

func NewListTopicCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var listTopicCmd = &cobra.Command{
		Use:   "list",
		Short: "Get list of all existing topics",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(configPath)
			adminClient := client.NewAdmin(adminConfig)
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

func NewDescribeTopicCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var describeTopicCmd = &cobra.Command{
		Use:   "describe",
		Short: "Describe topic",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(configPath)
			adminClient := client.NewAdmin(adminConfig)
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

			fmt.Printf("Topic: %s, Topic meta: %s, Num partitions: %d, replication factor: %d", response.Topic.Name,
				response.Topic.Description, response.Topic.NumPartitions, response.Topic.ReplicationFactor)
		},
	}

	describeTopicCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to describe")
	describeTopicCmd.MarkFlagRequired("topic")
	return describeTopicCmd
}
