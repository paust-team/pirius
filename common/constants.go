package common

import (
	"fmt"
	"os"
)

var (
	DefaultHomeDir                  = os.ExpandEnv("$HOME/.shapleq")
	DefaultBrokerConfigDir          = fmt.Sprintf("%s/config/broker", DefaultHomeDir)
	DefaultAdminConfigDir           = fmt.Sprintf("%s/config/admin", DefaultHomeDir)
	DefaultProducerConfigDir        = fmt.Sprintf("%s/config/producer", DefaultHomeDir)
	DefaultConsumerConfigDir        = fmt.Sprintf("%s/config/consumer", DefaultHomeDir)
	DefaultBrokerPort        uint   = 1101
	DefaultTimeout           uint   = 3
	DefaultConfigName        string = "config"
)

type BackPressure int

const (
	AtMostOnce BackPressure = iota
	AtLeastOnce
	ExactlyONce
)
