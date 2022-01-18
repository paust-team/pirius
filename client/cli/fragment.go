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

	topicFragmentCmd.Flags().StringVarP(&configPath, "config-path", "i", common.DefaultAdminConfigPath, "admin client config path")
	topicFragmentCmd.Flags().StringVar(&bootstrapServers, "broker-address", "", "broker address to connect (ex. localhost:1101)")
	topicFragmentCmd.Flags().IntVar(&timeout, "broker-timeout", 0, "connection timeout (milliseconds)")
	topicFragmentCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")

	adminConfig := config.NewAdminConfig()

	adminConfig.BindPFlags(topicFragmentCmd.Flags())
	adminConfig.BindPFlag("bootstrap.servers", topicFragmentCmd.Flags().Lookup("broker-address"))

	topicFragmentCmd.AddCommand(
		NewCreateTopicFragmentCmd(adminConfig),
	)

	return topicFragmentCmd
}

func NewCreateTopicFragmentCmd(adminConfig *config.AdminConfig) *cobra.Command {

	var createTopicFragmentCmd = &cobra.Command{
		Use:   "create",
		Short: "Create topic fragment",
		Run: func(cmd *cobra.Command, args []string) {

			adminConfig.Load(configPath)
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
