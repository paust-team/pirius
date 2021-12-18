package config

import (
	"fmt"
	logger "github.com/paust-team/shapleq/log"
	"github.com/spf13/viper"
	"strings"
)

type ClientConfigBase struct {
	*viper.Viper
}

var (
	defaultLogLevel            = logger.Info
	defaultBootstrapServerHost = "localhost:2181"
	defaultTimeout             = 3000
)

func NewClientConfigBase() *ClientConfigBase {

	v := viper.New()

	v.SetDefault("log-level", logger.LogLevelToString(defaultLogLevel))
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("bootstrap", map[string]interface{}{
		"servers": defaultBootstrapServerHost,
		"timeout": defaultTimeout,
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

func (c ClientConfigBase) BrokerTimeout() int {
	return c.GetInt("broker-timeout")
}

func (c *ClientConfigBase) SetBrokerTimeout(timeout int) {
	c.Set("broker-timeout", timeout)
}

func (c *ClientConfigBase) ServerAddresses() []string {
	addresses := strings.Split(c.GetString("bootstrap.servers"), ",")
	return addresses
}

func (c *ClientConfigBase) SetServerAddresses(addresses []string) {
	c.Set("bootstrap.servers", strings.Join(addresses, ","))
}

func (c *ClientConfigBase) BootstrapTimeout() int {
	return c.GetInt("bootstrap.timeout")
}

func (c *ClientConfigBase) SetBootstrapTimeout(timeout int) {
	c.Set("bootstrap.timeout", timeout)
}
