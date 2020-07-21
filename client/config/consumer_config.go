package config

type ConsumerConfig struct {
	*ClientConfigBase
}

func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{NewClientConfigBase()}
}
