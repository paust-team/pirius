package config

import (
	"fmt"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/network"
	"github.com/spf13/viper"
	"strings"
)

var (
	defaultLogDir                      = fmt.Sprintf("%s/log", common.DefaultHomeDir)
	defaultDataDir                     = fmt.Sprintf("%s/data", common.DefaultHomeDir)
	defaultLogLevel                    = logger.Info
	defaultZKPort                      = 2181
	defaultTimeout                     = 10000
	defaultZKHost                      = "localhost"
	defaultZKTimeout              uint = 3000
	defaultZKFlushInterval             = 2000
	defaultRetentionCheckInterval uint = 10000
)

type BrokerConfig struct {
	*viper.Viper
}

func NewBrokerConfig() *BrokerConfig {

	v := viper.New()

	host, err := network.GetOutboundIP()
	if err != nil {
		panic(err)
	}

	v.SetDefault("hostname", host.String())
	v.SetDefault("port", common.DefaultBrokerPort)
	v.SetDefault("log-dir", defaultLogDir)
	v.SetDefault("data-dir", defaultDataDir)
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("log-level", logger.LogLevelToString(defaultLogLevel))
	v.SetDefault("zookeeper", map[string]interface{}{
		"port":           defaultZKPort,
		"host":           defaultZKHost,
		"timeout":        defaultZKTimeout,
		"flush-interval": defaultZKFlushInterval,
	})
	v.SetDefault("retention-check-interval", defaultRetentionCheckInterval)

	return &BrokerConfig{v}
}

func (b *BrokerConfig) Load(configPath string) *BrokerConfig {
	b.SetConfigFile(configPath)
	err := b.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	return b
}

func (b BrokerConfig) Timeout() int {
	return b.GetInt("timeout")
}

func (b *BrokerConfig) SetTimeout(timeout int) {
	b.Set("timeout", timeout)
}

func (b BrokerConfig) Hostname() string {
	return b.GetString("hostname")
}

func (b *BrokerConfig) SetHostname(name string) {
	b.Set("hostname", name)
}

func (b BrokerConfig) Port() uint {
	return b.GetUint("port")
}

func (b *BrokerConfig) SetPort(port uint) {
	b.Set("port", port)
}

func (b BrokerConfig) LogDir() string {
	return common.ReplaceTildeToHomePath(b.GetString("log-dir"))
}

func (b *BrokerConfig) SetLogDir(logDir string) {
	b.Set("log-dir", logDir)
}

func (b BrokerConfig) DataDir() string {
	return common.ReplaceTildeToHomePath(b.GetString("data-dir"))
}

func (b *BrokerConfig) SetDataDir(dataDir string) {
	b.Set("data-dir", dataDir)
}

func (b BrokerConfig) ZKQuorum() []string {
	addresses := strings.Split(b.GetString("zookeeper.quorum"), ",")
	return addresses
}

func (b *BrokerConfig) SetZKQuorum(addresses []string) {
	b.Set("zookeeper.quorum", strings.Join(addresses, ","))
}

func (b BrokerConfig) ZKTimeout() uint {
	return b.GetUint("zookeeper.timeout")
}

func (b *BrokerConfig) SetZKTimeout(timeout uint) {
	b.Set("zookeeper.timeout", timeout)
}

func (b BrokerConfig) LogLevel() logger.LogLevel {
	return logger.LogLevelFromString(b.GetString("log-level"))
}

func (b *BrokerConfig) SetLogLevel(logLevel logger.LogLevel) {
	b.Set("log-level", logger.LogLevelToString(logLevel))
}

func (b BrokerConfig) ZKFlushInterval() uint {
	return b.GetUint("zookeeper.flush-interval")
}

func (b *BrokerConfig) SetZKFlushInterval(interval uint) {
	b.Set("zookeeper.flush-interval", interval)
}

func (b BrokerConfig) RetentionCheckInterval() uint {
	return b.GetUint("retention-check-interval")
}

func (b *BrokerConfig) SetRetentionCheckInterval(interval uint) {
	b.Set("retention-check-interval", interval)
}
