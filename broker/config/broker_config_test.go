package config

import (
	"github.com/paust-team/shapleq/common"
	"testing"
)

func TestBrokerConfigLoad(t *testing.T) {
	brokerConfig := NewBrokerConfig()

	defaultZKTimeout := brokerConfig.ZKTimeout()

	brokerConfig.Load("./config.yml")
	if brokerConfig.ZKTimeout() == defaultZKTimeout {
		t.Errorf("value in file is not wrapped")
	}
}

func TestBrokerConfigSet(t *testing.T) {
	brokerConfig := NewBrokerConfig().Load("./config.yml")

	if brokerConfig.Port() != common.DefaultBrokerPort {
		t.Errorf("wrong default value")
	}

	var expectedPort uint = 11010
	brokerConfig.SetPort(expectedPort)

	if brokerConfig.Port() != expectedPort {
		t.Errorf("value is not set")
	}
}

func TestBrokerConfigStructured(t *testing.T) {
	brokerConfig := NewBrokerConfig().Load("./config.yml")

	expectedAddress1 := "172.0.0.1:2181"
	expectedAddress2 := "172.0.0.2:2181"

	brokerConfig.SetZKAddresses([]string{expectedAddress1, expectedAddress2})
	addresses := brokerConfig.ZKAddresses()
	if addresses[0] != expectedAddress1 || addresses[1] != expectedAddress2 {
		t.Errorf("value is not set")
	}
}
