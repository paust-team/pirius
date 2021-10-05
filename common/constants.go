package common

import (
	"fmt"
	"github.com/google/uuid"
	"os"
	"strings"
)

var (
	DefaultHomeDir                 = os.ExpandEnv("$HOME/.shapleq")
	DefaultBrokerConfigPath        = fmt.Sprintf("%s/config/broker/config.yml", DefaultHomeDir)
	DefaultAdminConfigPath         = fmt.Sprintf("%s/config/admin/config.yml", DefaultHomeDir)
	DefaultProducerConfigPath      = fmt.Sprintf("%s/config/producer/config.yml", DefaultHomeDir)
	DefaultConsumerConfigPath      = fmt.Sprintf("%s/config/consumer/config.yml", DefaultHomeDir)
	DefaultBrokerPort         uint = 1101
)

type BackPressure int

const (
	AtMostOnce BackPressure = iota
	AtLeastOnce
	ExactlyONce
)

func GenerateNodeId() string {
	id, _ := uuid.NewUUID()
	return strings.Replace(id.String(), "-", "", -1)
}
