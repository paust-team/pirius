package zookeeper

import (
	"fmt"
	"os"
	"testing"
)

var zkClient *ZKClient

func TestMain(m *testing.M) {
	zkClient = NewZKClient("127.0.0.1")
	err := zkClient.Connect()

	if err != nil {
		fmt.Println("cannot connect zookeeper")
		return
	}

	err = zkClient.CreatePathsIfNotExist()
	if err != nil {
		fmt.Println("cannot create paths")
		return
	}

	code := m.Run()

	zkClient.RemoveAllPath()
	zkClient.Close()
	os.Exit(code)
}

func TestZKClient_AddBroker(t *testing.T) {
	host, err := GetOutboundIP()
	if err != nil {
		t.Error(err)
	}

	if err = zkClient.AddBroker(host.String()); err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetBrokers()
	if err != nil {
		t.Error(err)
	}

	if brokers[0] != host.String() {
		t.Error("failed to add broker ", host.String())
	}
}

func TestZKClient_AddTopic(t *testing.T) {
	expected := "topic1"
	err := zkClient.AddTopic(expected)
	if err != nil {
		t.Error(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	if topics[len(topics)-1] != expected {
		t.Error("failed to add topic ", expected)
	}

	expected = "topic2"
	err = zkClient.AddTopic(expected)
	if err != nil {
		t.Error(err)
	}

	topics, err = zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	if topics[len(topics)-1] != expected {
		t.Error("failed to add topic ", expected)
	}
}

func TestZKClient_AddTopicBroker(t *testing.T) {
	expected := "topic3"
	err := zkClient.AddTopic(expected)
	if err != nil {
		t.Error(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	if topics[len(topics)-1] != expected {
		t.Error("failed to add topic ", expected)
	}

	host, err := GetOutboundIP()
	if err != nil {
		t.Error(err)
	}

	err = zkClient.AddTopicBroker(expected, host.String())
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetTopicBrokers(expected)
	if err != nil {
		t.Error(err)
	}

	if brokers[len(brokers)-1] != host.String() {
		t.Error("failed to add topic broker ", host)
	}
}

func TestZKClient_RemoveTopic(t *testing.T) {
	topic := "topic5"
	err := zkClient.AddTopic(topic)
	if err != nil {
		t.Error(err)
	}

	err = zkClient.RemoveTopic(topic)
	if err != nil {
		t.Error(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	for _, top := range topics {
		if top == topic {
			t.Errorf("topic did not deleted")
		}
	}
}

func TestZKClient_RemoveBroker(t *testing.T) {
	broker := "127.0.0.1"
	err := zkClient.AddBroker(broker)
	if err != nil {
		t.Error(err)
	}

	err = zkClient.RemoveBroker(broker)
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetBrokers()

	for _, b := range brokers {
		if b == broker {
			t.Errorf("broker did not deleted")
		}
	}
}

func TestZKClient_RemoveTopicBroker(t *testing.T) {
	topic := "topic4"
	err := zkClient.AddTopic(topic)
	if err != nil {
		t.Error(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	if topics[len(topics)-1] != topic {
		t.Error("failed to add topic ", topic)
	}

	host, err := GetOutboundIP()
	if err != nil {
		t.Error(err)
	}

	err = zkClient.AddTopicBroker(topic, host.String())
	if err != nil {
		t.Error(err)
	}

	err = zkClient.RemoveTopicBroker(topic, host.String())
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetTopicBrokers(topic)
	if err != nil {
		t.Error(err)
	}

	for _, broker := range brokers {
		if broker == host.String() {
			t.Errorf("topic broker did not deleted")
		}
	}
}
