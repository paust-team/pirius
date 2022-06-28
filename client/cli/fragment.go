package cli

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	"github.com/spf13/cobra"
	"os"
)

func NewTopicFragmentCmd() *cobra.Command {

	var topicFragmentCmd = &cobra.Command{
		Use:   "fragment",
		Short: "topic fragment commands",
	}

	topicFragmentCmd.PersistentFlags().StringVarP(&adminConfigPath, "config-path", "i", common.DefaultAdminConfigPath, "admin client config path")
	topicFragmentCmd.PersistentFlags().StringVar(&bootstrapServers, "broker-address", "", "broker address to connect (ex. localhost:1101)")
	topicFragmentCmd.PersistentFlags().IntVar(&timeout, "broker-timeout", 0, "connection timeout (milliseconds)")
	topicFragmentCmd.PersistentFlags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")

	adminConfig := config.NewAdminConfig()

	adminConfig.BindPFlags(topicFragmentCmd.PersistentFlags())
	adminConfig.BindPFlag("bootstrap.servers", topicFragmentCmd.PersistentFlags().Lookup("broker-address"))

	topicFragmentCmd.AddCommand(
		NewCreateTopicFragmentCmd(adminConfig),
		NewDeleteTopicFragmentCmd(adminConfig),
		NewDescribeTopicFragmentCmd(adminConfig),
	)

	return topicFragmentCmd
}

func NewCreateTopicFragmentCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var createTopicFragmentCmd = &cobra.Command{
		Use:   "create",
		Short: "Create topic fragment",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(adminConfigPath)
			adminClient := client.NewAdmin(adminConfig)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			fragment, err := adminClient.CreateFragment(topicName)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Printf("topic fragment created with id(%d)", fragment.Id)
		},
	}

	createTopicFragmentCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to create a fragment")
	createTopicFragmentCmd.MarkFlagRequired("topic")

	return createTopicFragmentCmd
}

func NewDeleteTopicFragmentCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var createTopicFragmentCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete topic fragment",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(adminConfigPath)
			adminClient := client.NewAdmin(adminConfig)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			err := adminClient.DeleteFragment(topicName, fragmentId)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Printf("topic fragment(%d) is deleted", fragmentId)
		},
	}

	createTopicFragmentCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to create a fragment")
	createTopicFragmentCmd.Flags().Uint32VarP(&fragmentId, "fragment", "r", 0, "fragment id to delete")

	createTopicFragmentCmd.MarkFlagRequired("topic")
	createTopicFragmentCmd.MarkFlagRequired("fragment")

	return createTopicFragmentCmd
}

func NewDescribeTopicFragmentCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var describeTopicFragmentCmd = &cobra.Command{
		Use:   "describe",
		Short: "Describe topic fragment",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(adminConfigPath)
			adminClient := client.NewAdmin(adminConfig)
			defer adminClient.Close()

			if err := adminClient.Connect(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			fragment, err := adminClient.DescribeFragment(topicName, fragmentId)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Printf("Id: %d, LastOffset: %d, Brokers: %s", fragment.Id, fragment.LastOffset, fragment.BrokerAddresses)
		},
	}

	describeTopicFragmentCmd.Flags().StringVarP(&topicName, "topic", "n", "", "topic name to create a fragment")
	describeTopicFragmentCmd.Flags().Uint32VarP(&fragmentId, "fragment", "r", 0, "fragment id to delete")

	describeTopicFragmentCmd.MarkFlagRequired("topic")
	describeTopicFragmentCmd.MarkFlagRequired("fragment")

	return describeTopicFragmentCmd
}
