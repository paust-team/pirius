package zookeeper

import (
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	"os"
	"testing"
)

var zkClient *ZKClient

func TestMain(m *testing.M) {
	zkClient = NewZKClient([]string{"127.0.0.1:2181"}, 3000)
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
	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"
	if err = zkClient.AddBroker(brokerAddr); err != nil {
		t.Fatal(err)
	}

	brokers, err := zkClient.GetBrokers()
	if err != nil {
		t.Fatal(err)
	}

	if len(brokers) == 0 {
		t.Fatal("no brokers")
	}
	if brokers[0] != brokerAddr {
		t.Error("failed to add broker ", brokerAddr)
	}
}

func TestZKClient_AddTopic(t *testing.T) {
	expectedTopic := "topic1"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(expectedTopic, topicValue)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("no topics")
	}

	if topics[len(topics)-1] != expectedTopic {
		t.Fatal("failed to add topic", expectedTopic, topics[len(topics)-1])
	}

	targetTopicValue, err := zkClient.GetTopic(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if targetTopicValue.TopicMeta() != expectedTopicMeta {
		t.Fatal("topic meta not matched ", expectedTopicMeta, targetTopicValue.TopicMeta())
	}

	if targetTopicValue.NumPartitions() != expectedNumPartitions {
		t.Fatal("num partition not matched ", expectedNumPartitions, targetTopicValue.NumPartitions())
	}

	if targetTopicValue.ReplicationFactor() != expectedReplicationFactor {
		t.Fatal("replication factor not matched ", expectedReplicationFactor, targetTopicValue.ReplicationFactor())
	}

}

func TestZKClient_AddTopicBroker(t *testing.T) {
	expectedTopic := "topic3"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(expectedTopic, topicValue)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("no topics")
	}
	if topics[len(topics)-1] != expectedTopic {
		t.Fatal("failed to add topic ", expectedTopic)
	}

	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"
	err = zkClient.AddTopicBroker(expectedTopic, brokerAddr)
	if err != nil {
		t.Fatal(err)
	}

	brokers, err := zkClient.GetTopicBrokers(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if brokers[len(brokers)-1] != brokerAddr {
		t.Fatal("failed to add topic broker ", host)
	}
}

func TestZKClient_RemoveTopic(t *testing.T) {
	topic := "topic5"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(topic, topicValue)
	if err != nil {
		t.Fatal(err)
	}

	err = zkClient.RemoveTopic(topic)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}

	for _, top := range topics {
		if top == topic {
			t.Error("topic did not deleted")
		}
	}
}

func TestZKClient_RemoveBroker(t *testing.T) {
	broker := "127.0.0.1:1101"
	err := zkClient.AddBroker(broker)
	if err != nil {
		t.Fatal(err)
	}

	err = zkClient.RemoveBroker(broker)
	if err != nil {
		t.Fatal(err)
	}

	brokers, err := zkClient.GetBrokers()

	for _, b := range brokers {
		if b == broker {
			t.Error("broker did not deleted")
		}
	}
}

func TestZKClient_RemoveTopicBroker(t *testing.T) {
	topic := "topic4"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1

	topicValue := internals.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor)
	err := zkClient.AddTopic(topic, topicValue)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := zkClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}

	if topics[len(topics)-1] != topic {
		t.Fatal("failed to add topic ", topic)
	}

	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"

	err = zkClient.AddTopicBroker(topic, brokerAddr)
	if err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
	}

	err = zkClient.RemoveTopicBroker(topic, brokerAddr)
	if err != nil {
		t.Fatal(err)
	}

	brokers, err := zkClient.GetTopicBrokers(topic)
	if err != nil {
		t.Fatal(err)
	}

	for _, broker := range brokers {
		if broker == host.String() {
			t.Error("topic broker did not deleted")
		}
	}
}
