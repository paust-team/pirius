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
	defaultTimeout    = 3000
)

func NewClientConfigBase() *ClientConfigBase {

	v := viper.New()

	v.SetDefault("log-level", logger.LogLevelToString(defaultLogLevel))
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("broker", map[string]interface{}{
		"port": common.DefaultBrokerPort,
		"host": defaultBrokerHost,
	})

	return &ClientConfigBase{v}
}

func (c *ClientConfigBase) Load(configPath string) *ClientConfigBase {
	c.SetConfigFile(configPath)
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
	return fmt.Sprintf("%s:%d", c.GetString("broker.host"), c.GetUint("broker.port"))
}

func (c *ClientConfigBase) SetBrokerHost(host string) {
	c.Set("broker.host", host)
}

func (c *ClientConfigBase) SetBrokerPort(port uint) {
	c.Set("broker.port", port)
}
