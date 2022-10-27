package constants

import (
	"fmt"
)

var (
	DefaultLogDir                  = fmt.Sprintf("%s/log", DefaultHomeDir)
	DefaultDataDir                 = fmt.Sprintf("%s/data", DefaultHomeDir)
	DefaultBrokerConfigPath        = fmt.Sprintf("%s/config/broker/config.yml", DefaultHomeDir)
	DefaultAdminConfigPath         = fmt.Sprintf("%s/config/admin/config.yml", DefaultHomeDir)
	DefaultProducerConfigPath      = fmt.Sprintf("%s/config/producer/config.yml", DefaultHomeDir)
	DefaultConsumerConfigPath      = fmt.Sprintf("%s/config/consumer/config.yml", DefaultHomeDir)
	DefaultAgentPort          uint = 11010
)

const AgentMetaFileName = "agent.gob"
const MaxFragmentCount = 1 << 8

const MinRetentionPeriod = 1
const MaxRetentionPeriod = 30
