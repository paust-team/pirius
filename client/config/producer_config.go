package config

type ProducerConfig struct {
	*ClientConfigBase
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{NewClientConfigBase()}
}
