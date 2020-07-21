package config

import (
	"fmt"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
	"github.com/spf13/viper"
)

var (
	defaultLogDir   = fmt.Sprintf("%s/log", common.DefaultHomeDir)
	defaultDataDir  = fmt.Sprintf("%s/data", common.DefaultHomeDir)
	defaultLogLevel = logger.Info
)

type BrokerConfig struct {
	*viper.Viper
}

func NewBrokerConfig() *BrokerConfig {

	v := viper.New()
	v.SetConfigName(common.DefaultConfigName)

	v.SetDefault("port", common.DefaultBrokerPort)
	v.SetDefault("log-dir", defaultLogDir)
	v.SetDefault("data-dir", defaultDataDir)
	v.SetDefault("log-level", logger.LogLevelToString(defaultLogLevel))
	v.SetDefault("zookeeper", map[string]interface{}{
		"port":    zookeeper.DefaultPort,
		"host":    zookeeper.DefaultHost,
		"timeout": zookeeper.DefaultTimeout,
	})

	return &BrokerConfig{v}
}

func (b *BrokerConfig) Load(configDir string) *BrokerConfig {
	b.SetConfigType("yaml")
	b.AddConfigPath(configDir)
	err := b.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	return b
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
	zkInfo := b.GetStringMap("zookeeper")
	return fmt.Sprintf("%s:%d", zkInfo["host"], zkInfo["port"])
}

func (b *BrokerConfig) SetZKHost(zkHost string) {
	zkInfo := b.GetStringMap("zookeeper")
	zkInfo["host"] = zkHost
	b.Set("zookeeper", zkInfo)
}

func (b *BrokerConfig) SetZKPort(zkPort int) {
	zkInfo := b.GetStringMap("zookeeper")
	zkInfo["port"] = zkPort
	b.Set("zookeeper", zkInfo)
}

func (b BrokerConfig) ZKTimeout() int {
	zkInfo := b.GetStringMap("zookeeper")
	return zkInfo["timeout"].(int)
}

func (b BrokerConfig) LogLevel() logger.LogLevel {
	return logger.LogLevelFromString(b.GetString("log-level"))
}

func (b *BrokerConfig) SetLogLevel(logLevel logger.LogLevel) {
	b.Set("log-level", logger.LogLevelToString(logLevel))
}