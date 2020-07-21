package config

import (
	"fmt"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/spf13/viper"
)

type ClientConfigBase struct {
	*viper.Viper
}

var (
	defaultLogLevel   = logger.Info
	defaultBrokerHost = "localhost"
	defaultTimeout    = 3
)

func NewClientConfigBase() *ClientConfigBase {

	v := viper.New()
	v.SetConfigName(common.DefaultConfigName)

	v.SetDefault("log-level", logger.LogLevelToString(defaultLogLevel))
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("broker", map[string]interface{}{
		"port": common.DefaultBrokerPort,
		"host": defaultBrokerHost,
	})

	return &ClientConfigBase{v}
}

func (c *ClientConfigBase) Load(configDir string) *ClientConfigBase {
	c.SetConfigType("yaml")
	c.AddConfigPath(configDir)
	err := c.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	return c
}

func (c ClientConfigBase) LogLevel() logger.LogLevel {
	return logger.LogLevelFromString(c.GetString("log-level"))
}

func (c *ClientConfigBase) SetLogLevel(logLevel logger.LogLevel) {
	c.Set("log-level", logger.LogLevelToString(logLevel))
}

func (c ClientConfigBase) Timeout() int {
	return c.GetInt("timeout")
}

func (c *ClientConfigBase) SetTimeout(timeout int) {
	c.Set("timeout", timeout)
}

func (c *ClientConfigBase) BrokerAddr() string {
	brokerInfo := c.GetStringMap("broker")
	return fmt.Sprintf("%s:%d", brokerInfo["host"], brokerInfo["port"])
}

func (c *ClientConfigBase) SetBrokerHost(host string) {
	brokerInfo := c.GetStringMap("broker")
	brokerInfo["host"] = host
	c.Set("broker", brokerInfo)
}

func (c *ClientConfigBase) SetBrokerPort(port uint) {
	brokerInfo := c.GetStringMap("broker")
	brokerInfo["port"] = port
	c.Set("broker", brokerInfo)
}