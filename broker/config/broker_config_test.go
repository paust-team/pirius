package config

import (
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

	brokerConfig.SetPort(11010)

	if brokerConfig.Port() != 11010 {
		t.Errorf("value is not set")
	}
}
