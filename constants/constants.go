package constants

import (
	"fmt"
)

var (
	DefaultLogDir                = fmt.Sprintf("%s/log", DefaultHomeDir)
	DefaultDataDir               = fmt.Sprintf("%s/data", DefaultHomeDir)
	DefaultBrokerConfigPath      = fmt.Sprintf("%s/config/broker/config.yml", DefaultHomeDir)
	DefaultAgentConfigPath       = fmt.Sprintf("%s/config/agent/config.yml", DefaultHomeDir)
	DefaultAgentPort        uint = 11010
	DefaultBrokerPort       uint = 1101
)

const AgentMetaFileName = "agent.gob"
const MaxFragmentCount = 1 << 8

const MinRetentionPeriod = 1
const MaxRetentionPeriod = 30

const WatchEventBuffer = 5
const InitialRebalanceTimeout = 3
