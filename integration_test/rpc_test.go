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
	brokerConfig.SetZKQuorum(zkAddrs)
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
	adminConfig.SetServerAddresses(brokerAddrs)
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

func TestCreateTopicAndFragment(t *testing.T) {

	// Start broker
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKQuorum(zkAddrs)
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
	adminConfig.SetServerAddresses(brokerAddrs)
	adminClient := client.NewAdmin(adminConfig)
	defer adminClient.Close()

	if err := adminClient.Connect(); err != nil {
		t.Error(err)
		return
	}

	expectedTopicName := "test-tp1"
	expectedDescription := "test-description"
	err := adminClient.CreateTopic(expectedTopicName, expectedDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(expectedTopicName)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	fmt.Println("created fragment id = ", fragment.Id)
}

func TestDeleteTopicAndFragment(t *testing.T) {

	// Start broker
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKQuorum(zkAddrs)
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
	adminConfig.SetServerAddresses(brokerAddrs)
	adminClient := client.NewAdmin(adminConfig)
	defer adminClient.Close()

	if err := adminClient.Connect(); err != nil {
		t.Error(err)
		return
	}

	expectedTopicName := "test-tp2"
	expectedDescription := "test-description"
	err := adminClient.CreateTopic(expectedTopicName, expectedDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(expectedTopicName)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	if err = adminClient.DeleteFragment(expectedTopicName, fragment.Id); err != nil {
		t.Fatal(err)
	}

	// TODO:: get list of fragments and check whether the fragment is deleted

	if err = adminClient.DeleteTopic(expectedTopicName); err != nil {
		t.Fatal(err)
	}
	res, err := adminClient.DescribeTopic(expectedTopicName)
	fmt.Println(res.Topic.Name)

}
