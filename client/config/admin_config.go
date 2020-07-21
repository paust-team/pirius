package config

type AdminConfig struct {
	*ClientConfigBase
}

func NewAdminConfig() *AdminConfig {
	return &AdminConfig{NewClientConfigBase()}
}
