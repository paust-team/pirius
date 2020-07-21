package config

import (
	"fmt"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/zookeeper"
	"testing"
)

func TestBrokerConfigLoad(t *testing.T) {
	brokerConfig := NewBrokerConfig()

	if brokerConfig.ZKTimeout() != zookeeper.DefaultTimeout {
		t.Errorf("wrong default value")
	}

	brokerConfig.Load("./")
	if brokerConfig.ZKTimeout() == zookeeper.DefaultTimeout {
		t.Errorf("value in file is not wrapped")
	}
}

func TestBrokerConfigSet(t *testing.T) {
	brokerConfig := NewBrokerConfig().Load("./")

	if brokerConfig.Port() != common.DefaultBrokerPort {
		t.Errorf("wrong default value")
	}

	var expectedPort uint = 11010
	brokerConfig.SetPort(expectedPort)

	if brokerConfig.Port() != expectedPort {
		t.Errorf("value is not set")
	}
}

func TestBrokerConfigStringMap(t *testing.T) {
	brokerConfig := NewBrokerConfig().Load("./")

	if brokerConfig.ZKAddr() != fmt.Sprintf("%s:%d", zookeeper.DefaultHost, zookeeper.DefaultPort) {
		t.Errorf("wrong default value")
	}

	expectedHost := "172.0.0.1"
	var expectedPort uint = 10000

	brokerConfig.SetZKHost(expectedHost)
	brokerConfig.SetZKPort(expectedPort)

	if brokerConfig.ZKAddr() != fmt.Sprintf("%s:%d", expectedHost, expectedPort) {
		t.Errorf("value is not set")
	}
}
