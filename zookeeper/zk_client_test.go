package zookeeper

import (
	"fmt"
	"github.com/paust-team/paustq/broker/internals"
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
	expectedTopic := "topic1"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicValueFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(expectedTopic, topicValue)
	if err != nil {
		t.Error(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	if topics[len(topics)-1] != expectedTopic {
		t.Error("failed to add topic ", expectedTopic)
	}

	targetTopicValue, err := zkClient.GetTopic(expectedTopic)
	if err != nil {
		t.Error(err)
	}

	if targetTopicValue.TopicMeta() != expectedTopicMeta {
		t.Error("topic meta not matched ", expectedTopicMeta, targetTopicValue.TopicMeta())
	}

	if targetTopicValue.NumPartitions() != expectedNumPartitions {
		t.Error("num partition not matched ", expectedNumPartitions, targetTopicValue.NumPartitions())
	}

	if targetTopicValue.ReplicationFactor() != expectedReplicationFactor {
		t.Error("replication factor not matched ", expectedReplicationFactor, targetTopicValue.ReplicationFactor())
	}

}

func TestZKClient_AddTopicBroker(t *testing.T) {
	expectedTopic := "topic3"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicValueFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(expectedTopic, topicValue)
	if err != nil {
		t.Error(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Error(err)
	}

	if topics[len(topics)-1] != expectedTopic {
		t.Error("failed to add topic ", expectedTopic)
	}

	host, err := GetOutboundIP()
	if err != nil {
		t.Error(err)
	}

	err = zkClient.AddTopicBroker(expectedTopic, host.String())
	if err != nil {
		t.Error(err)
	}

	brokers, err := zkClient.GetTopicBrokers(expectedTopic)
	if err != nil {
		t.Error(err)
	}

	if brokers[len(brokers)-1] != host.String() {
		t.Error("failed to add topic broker ", host)
	}
}

func TestZKClient_RemoveTopic(t *testing.T) {
	topic := "topic5"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicValueFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(topic, topicValue)
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
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicValueFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(topic, topicValue)
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
