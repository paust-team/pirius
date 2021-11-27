package integration_test

import (
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/client"
	config2 "github.com/paust-team/shapleq/client/config"
	"sync"
	"testing"
)

func TestHeartBeat(t *testing.T) {

	// Start broker
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKAddresses(zkAddr)
	brokerInstance := broker.NewBroker(brokerConfig)
	bwg := sync.WaitGroup{}
	bwg.Add(1)

	defer brokerInstance.Clean()
	defer bwg.Wait()
	defer brokerInstance.Stop()

	go func() {
		defer bwg.Done()
		brokerInstance.Start()
	}()

	Sleep(1)

	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(testLogLevel)
	adminConfig.SetBrokerHost(brokerHost)
	adminConfig.SetBrokerPort(brokerPort)
	adminClient := client.NewAdmin(adminConfig)
	defer adminClient.Close()

	if err := adminClient.Connect(); err != nil {
		t.Error(err)
		return
	}

	pongMsg, err := adminClient.Heartbeat("test", 1)
	if err != nil {
		t.Error(pongMsg)
		return
	}
	fmt.Println(pongMsg.Echo)

}
