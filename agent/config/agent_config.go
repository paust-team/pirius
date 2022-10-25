package config

import (
	"fmt"
	"github.com/paust-team/shapleq/agent/utils"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
)

var (
	defaultLogLevel                    = zap.InfoLevel
	defaultTimeout                     = 10000
	defaultZKQuorum                    = "localhost:2181"
	defaultZKTimeout              uint = 3000
	defaultZKFlushInterval             = 2000
	defaultRetentionPeriod             = 1
	defaultRetentionCheckInterval uint = 10000
	defaultDBName                      = "shapleq-store"
	defaultBindAddr                    = "127.0.0.1"
)

type AgentConfig struct {
	*viper.Viper
}

func NewAgentConfig() *AgentConfig {

	v := viper.New()

	host, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}

	v.SetDefault("bind", defaultBindAddr)
	v.SetDefault("host", host.String())
	v.SetDefault("port", utils.DefaultAgentPort)
	v.SetDefault("log-dir", utils.DefaultLogDir)
	v.SetDefault("data-dir", utils.DefaultDataDir)
	v.SetDefault("timeout", defaultTimeout)
	v.SetDefault("log-level", defaultLogLevel)
	v.SetDefault("retention", defaultRetentionPeriod)
	v.SetDefault("db-name", defaultDBName)
	v.SetDefault("zookeeper", map[string]interface{}{
		"quorum":         defaultZKQuorum,
		"timeout":        defaultZKTimeout,
		"flush-interval": defaultZKFlushInterval,
	})

	return &AgentConfig{v}
}

func (b *AgentConfig) Load(configPath string) *AgentConfig {
	b.SetConfigFile(configPath)
	err := b.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	return b
}

func (b AgentConfig) Timeout() int {
	return b.GetInt("timeout")
}

func (b *AgentConfig) SetTimeout(timeout int) {
	b.Set("timeout", timeout)
}

func (b AgentConfig) BindAddress() string {
	return b.GetString("bind")
}

func (b *AgentConfig) SetBindAddress(name string) {
	b.Set("bind", name)
}

func (b AgentConfig) Host() string {
	return b.GetString("host")
}

func (b *AgentConfig) SetHost(name string) {
	b.Set("host", name)
}

func (b AgentConfig) Port() uint {
	return b.GetUint("port")
}

func (b *AgentConfig) SetPort(port uint) {
	b.Set("port", port)
}

func (b AgentConfig) LogDir() string {
	return utils.ReplaceTildeToHomePath(b.GetString("log-dir"))
}

func (b *AgentConfig) SetLogDir(logDir string) {
	b.Set("log-dir", logDir)
}

func (b AgentConfig) DataDir() string {
	return utils.ReplaceTildeToHomePath(b.GetString("data-dir"))
}

func (b *AgentConfig) SetDataDir(dataDir string) {
	b.Set("data-dir", dataDir)
}

func (b AgentConfig) DBName() string {
	return b.GetString("db-name")
}

func (b *AgentConfig) SetDBName(name string) {
	b.Set("db-name", name)
}

func (b AgentConfig) RetentionPeriod() uint32 {
	return b.GetUint32("retention")
}

func (b AgentConfig) SetRetentionPeriod(retention uint32) {
	b.Set("retention", retention)
}

func (b AgentConfig) ZKQuorum() []string {
	addresses := strings.Split(b.GetString("zookeeper.quorum"), ",")
	return addresses
}

func (b *AgentConfig) SetZKQuorum(addresses []string) {
	b.Set("zookeeper.quorum", strings.Join(addresses, ","))
}

func (b AgentConfig) ZKTimeout() uint {
	return b.GetUint("zookeeper.timeout")
}

func (b *AgentConfig) SetZKTimeout(timeout uint) {
	b.Set("zookeeper.timeout", timeout)
}

func (b AgentConfig) LogLevel() zapcore.Level {
	return zapcore.Level(b.GetUint("log-level"))
}

func (b *AgentConfig) SetLogLevel(logLevel zapcore.Level) {
	b.Set("log-level", logLevel)
}

func (b AgentConfig) ZKFlushInterval() uint {
	return b.GetUint("zookeeper.flush-interval")
}

func (b *AgentConfig) SetZKFlushInterval(interval uint) {
	b.Set("zookeeper.flush-interval", interval)
}

func (b AgentConfig) RetentionCheckInterval() uint {
	return b.GetUint("retention-check-interval")
}

func (b *AgentConfig) SetRetentionCheckInterval(interval uint) {
	b.Set("retention-check-interval", interval)
}