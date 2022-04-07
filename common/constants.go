package common

import (
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"path/filepath"
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

const MaxFragmentCount = 1 << 8

func GenerateFragmentId() uint32 {
	return uint32(rand.Intn(MaxFragmentCount)) + 1
}

func GenerateNodeId() string {
	id, _ := uuid.NewUUID()
	return strings.Replace(id.String(), "-", "", -1)
}

func ReplaceTildeToHomePath(dir string) string {
	if strings.HasPrefix(dir, "~/") {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, dir[2:])
	}
	return dir
}
