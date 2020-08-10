package config

import (
	"fmt"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/spf13/viper"
)

var (
	defaultLogDir         = fmt.Sprintf("%s/log", common.DefaultHomeDir)
	defaultDataDir        = fmt.Sprintf("%s/data", common.DefaultHomeDir)
	defaultLogLevel       = logger.Info
	defaultZKPort         = 2181
	defaultTimeout        = 10000
	defaultZKHost         = "localhost"
	defaultZKTimeout uint = 3000
)

type BrokerConfig struct {
	*viper.Viper
}

func NewBrokerConfig() *BrokerConfig {

	v := viper.New()

	v.SetDefault("port", common.DefaultBrokerPort)
	v.SetDefault("log-dir", defaultLogDir)
	v.SetDefault("data-dir", defaultDataDir)
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("log-level", logger.LogLevelToString(defaultLogLevel))
	v.SetDefault("zookeeper", map[string]interface{}{
		"port":    defaultZKPort,
		"host":    defaultZKHost,
		"timeout": defaultZKTimeout,
	})

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

func (b BrokerConfig) Port() uint {
	return b.GetUint("port")
}

func (b *BrokerConfig) SetPort(port uint) {
	b.Set("port", port)
}

func (b BrokerConfig) LogDir() string {
	return b.GetString("log-dir")
}

func (b *BrokerConfig) SetLogDir(logDir string) {
	b.Set("log-dir", logDir)
}

func (b BrokerConfig) DataDir() string {
	return b.GetString("data-dir")
}

func (b *BrokerConfig) SetDataDir(dataDir string) {
	b.Set("data-dir", dataDir)
}

func (b BrokerConfig) ZKAddr() string {
	return fmt.Sprintf("%s:%d", b.GetString("zookeeper.host"), b.GetInt("zookeeper.port"))
}

func (b *BrokerConfig) SetZKHost(zkHost string) {
	b.Set("zookeeper.host", zkHost)
}

func (b *BrokerConfig) SetZKPort(zkPort uint) {
	b.Set("zookeeper.port", zkPort)
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
