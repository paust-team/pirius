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

	zkClient.deleteAllPath()
	zkClient.Close()
	os.Exit(code)
}

func TestZKClient_AddBroker(t *testing.T) {
	err := zkClient.AddBroker(GetOutboundIP().String())
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetBrokers()
	if err != nil {
		t.Error(err)
	}

	if brokers[0] != GetOutboundIP().String() {
		t.Error("failed to add broker ", GetOutboundIP())
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

	err = zkClient.AddTopicBroker(expected, GetOutboundIP().String())
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetTopicBrokers(expected)
	if err != nil {
		t.Error(err)
	}

	if brokers[len(brokers)-1] != GetOutboundIP().String() {
		t.Error("failed to add topic broker ", GetOutboundIP())
	}
}

func TestZKClient_DeleteTopic(t *testing.T) {
	topic := "topic5"
	err := zkClient.AddTopic(topic)
	if err != nil {
		t.Error(err)
	}

	err = zkClient.DeleteTopic(topic)
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

func TestZKClient_DeleteBroker(t *testing.T) {
	broker := "127.0.0.1"
	err := zkClient.AddBroker(broker)
	if err != nil {
		t.Error(err)
	}

	err = zkClient.DeleteBroker(broker)
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

func TestZKClient_DeleteTopicBroker(t *testing.T) {
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

	err = zkClient.AddTopicBroker(topic, GetOutboundIP().String())
	if err != nil {
		t.Error(err)
	}

	err = zkClient.DeleteTopicBroker(topic, GetOutboundIP().String())
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetTopicBrokers(topic)
	if err != nil {
		t.Error(err)
	}

	for _, broker := range brokers {
		if broker == GetOutboundIP().String() {
			t.Errorf("topic broker did not deleted")
		}
	}
}
