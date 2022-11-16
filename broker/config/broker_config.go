package config

import (
	"fmt"
	"github.com/paust-team/shapleq/constants"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	defaultLogLevel       = zap.InfoLevel
	defaultTimeout        = 10000
	defaultZKQuorum       = []string{"127.0.0.1:2181"}
	defaultZKTimeout uint = 3000
	defaultBindAddr       = "127.0.0.1"
)

type BrokerConfig struct {
	*viper.Viper
}

func NewBrokerConfig() BrokerConfig {
	v := viper.New()

	v.SetDefault("bind", defaultBindAddr)
	v.SetDefault("host", defaultBindAddr)
	v.SetDefault("port", constants.DefaultBrokerPort)
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("log-level", defaultLogLevel)
	v.SetDefault("zookeeper", map[string]interface{}{
		"quorum":  defaultZKQuorum,
		"timeout": defaultZKTimeout,
	})

	return BrokerConfig{v}
}

func (b BrokerConfig) Load(configPath string) BrokerConfig {
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

func (b BrokerConfig) SetTimeout(timeout int) {
	b.Set("timeout", timeout)
}

func (b BrokerConfig) BindAddress() string {
	return b.GetString("bind")
}

func (b BrokerConfig) SetBindAddress(name string) {
	b.Set("bind", name)
}

func (b BrokerConfig) Host() string {
	return b.GetString("host")
}

func (b BrokerConfig) SetHost(name string) {
	b.Set("host", name)
}

func (b BrokerConfig) Port() uint {
	return b.GetUint("port")
}

func (b BrokerConfig) SetPort(port uint) {
	b.Set("port", port)
}

func (b BrokerConfig) ZKQuorum() []string {
	return b.GetStringSlice("zookeeper.quorum")
}

func (b BrokerConfig) SetZKQuorum(quorum []string) {
	b.Set("zookeeper.quorum", quorum)
}

func (b BrokerConfig) SetZKTimeout(timeout uint) {
	b.Set("zookeeper.timeout", timeout)
}

func (b BrokerConfig) ZKTimeout() uint {
	return b.GetUint("zookeeper.timeout")
}

func (b BrokerConfig) LogLevel() zapcore.Level {
	return zapcore.Level(b.GetUint("log-level"))
}

func (b BrokerConfig) SetLogLevel(logLevel zapcore.Level) {
	b.Set("log-level", logLevel)
}
