package cmd

import (
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/constants"
	"github.com/spf13/cobra"
)

var (
	configPath string
	logDir     string
	dataDir    string
	logLevel   uint8
	port       uint
	zkQuorum   string
	zkTimeout  uint
)

func NewStartCmd() *cobra.Command {

	agentConfig := config.NewAgentConfig()
	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start shapleq agent",
		Run: func(cmd *cobra.Command, args []string) {
			panic("stand-alone agent is not supported for now")
		},
	}

	startCmd.Flags().BoolP("daemon", "d", false, "run with daemon")
	startCmd.Flags().StringVarP(&configPath, "config-path", "i", constants.DefaultAgentConfigPath, "agent config directory")
	startCmd.Flags().UintVar(&port, "port", constants.DefaultAgentPort, "agent port")
	startCmd.Flags().StringVar(&logDir, "log-dir", "", "log directory")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "", "data directory")
	startCmd.Flags().Uint8Var(&logLevel, "log-level", 0, "set log level [0=debug|1=info|2=warning|3=error]")
	startCmd.Flags().StringVar(&zkQuorum, "zk-quorum", "", "zookeeper quorum")
	startCmd.Flags().UintVar(&zkTimeout, "zk-timeout", 0, "zookeeper timeout")
	startCmd.Flags().BoolP("clear", "c", false, "DANGER: use this option only if you intend to reset data directory after agent is terminated")

	agentConfig.BindPFlags(startCmd.Flags())
	agentConfig.BindPFlag("zookeeper.quorum", startCmd.Flags().Lookup("zk-quorum"))
	agentConfig.BindPFlag("zookeeper.timeout", startCmd.Flags().Lookup("zk-timeout"))

	return startCmd
}
