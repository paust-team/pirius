package zookeeper

import (
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	"os"
	"testing"
	"time"
)

var zkqClient *ZKQClient

func TestMain(m *testing.M) {
	zkqClient = NewZKQClient("127.0.0.1", 3000, 2000)
	err := zkqClient.Connect()

	if err != nil {
		fmt.Println("cannot connect zookeeper")
		return
	}

	err = zkqClient.CreatePathsIfNotExist()
	if err != nil {
		fmt.Println("cannot create paths")
		return
	}

	code := m.Run()
	zkqClient.RemoveAllPath()
	zkqClient.Close()
	os.Exit(code)
}

func TestZKClient_AddBroker(t *testing.T) {
	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"
	if err = zkqClient.AddBroker(brokerAddr); err != nil {
		t.Fatal(err)
	}

	brokers, err := zkqClient.GetBrokers()
	if err != nil {
		t.Fatal(err)
	}

	if len(brokers) == 0 {
		t.Fatal("no brokers")
	}
	if brokers[0] != brokerAddr {
		t.Error("failed to add broker ", host.String())
	}
}

func TestZKClient_AddTopic(t *testing.T) {
	expectedTopic := "topic1"
	expectedTopicDescription := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumPublishers uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicMetaFromValues(expectedTopicDescription, expectedNumPartitions, expectedReplicationFactor,
		expectedLastOffset, expectedNumPublishers, expectedNumSubscribers)

	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topics, err := zkqClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("no topics")
	}

	if topics[len(topics)-1] != expectedTopic {
		t.Fatal("failed to add topic", expectedTopic, topics[len(topics)-1])
	}

	targetTopicValue, err := zkqClient.GetTopicData(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if targetTopicValue.Description() != expectedTopicDescription {
		t.Fatal("topic description not matched ", expectedTopicDescription, targetTopicValue.Description())
	}

	if targetTopicValue.NumPartitions() != expectedNumPartitions {
		t.Fatal("num partition not matched ", expectedNumPartitions, targetTopicValue.NumPartitions())
	}

	if targetTopicValue.ReplicationFactor() != expectedReplicationFactor {
		t.Fatal("replication factor not matched ", expectedReplicationFactor, targetTopicValue.ReplicationFactor())
	}

	if targetTopicValue.LastOffset() != expectedLastOffset {
		t.Fatal("last offset not matched ", expectedLastOffset, targetTopicValue.LastOffset())
	}

	if targetTopicValue.NumPublishers() != expectedNumPublishers {
		t.Fatal("num publisher not matched ", expectedNumPublishers, targetTopicValue.NumPublishers())
	}

	if targetTopicValue.NumSubscribers() != expectedNumSubscribers {
		t.Fatal("num subscriber not matched ", expectedNumSubscribers, targetTopicValue.NumSubscribers())
	}
}

func TestZKClient_AddTopicBroker(t *testing.T) {
	expectedTopic := "topic3"
	expectedTopicMeta := "topicMeta1"
	var expectedNumPartitions uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumPublishers uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor,
		expectedLastOffset, expectedNumPublishers, expectedNumSubscribers)
	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topics, err := zkqClient.GetTopics()
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
	err = zkqClient.AddTopicBroker(expectedTopic, brokerAddr)
	if err != nil {
		t.Fatal(err)
	}

	brokers, err := zkqClient.GetTopicBrokers(expectedTopic)
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
	var expectedLastOffset uint64 = 1
	var expectedNumPublishers uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor,
		expectedLastOffset, expectedNumPublishers, expectedNumSubscribers)
	if err := zkqClient.AddTopic(topic, topicValue); err != nil {
		t.Fatal(err)
	}

	if err := zkqClient.RemoveTopic(topic); err != nil {
		t.Fatal(err)
	}

	topics, err := zkqClient.GetTopics()
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
	if err := zkqClient.AddBroker(broker); err != nil {
		t.Fatal(err)
	}

	if err := zkqClient.RemoveBroker(broker); err != nil {
		t.Fatal(err)
	}

	brokers, err := zkqClient.GetBrokers()
	if err != nil {
		t.Fatal(err)
	}
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
	var expectedLastOffset uint64 = 1
	var expectedNumPublishers uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicMetaFromValues(expectedTopicMeta, expectedNumPartitions, expectedReplicationFactor,
		expectedLastOffset, expectedNumPublishers, expectedNumSubscribers)
	err := zkqClient.AddTopic(topic, topicValue)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := zkqClient.GetTopics()
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

	if err := zkqClient.AddTopicBroker(topic, brokerAddr); err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
	}

	if err := zkqClient.RemoveTopicBroker(topic, brokerAddr); err != nil {
		t.Fatal(err)
	}

	brokers, err := zkqClient.GetTopicBrokers(topic)
	if err != nil {
		t.Fatal(err)
	}

	for _, broker := range brokers {
		if broker == host.String() {
			t.Error("topic broker did not deleted")
		}
	}
}

func TestZKQClient_IncreaseLastOffset(t *testing.T) {
	expectedTopic := "topic6"
	expectedTopicDescription := "topicMeta6"
	var initialLastOffset uint64 = 1
	var expectedLastOffset uint64 = 2

	topicValue := common.NewTopicMetaFromValues(expectedTopicDescription, 1, 1,
		initialLastOffset, 1, 1)

	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topics, err := zkqClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("no topics")
	}

	offset, err := zkqClient.IncreaseLastOffset(expectedTopic)
	if expectedLastOffset != offset {
		t.Errorf("Expected offset is %d, but got %d from memory", expectedLastOffset, offset)
	}

	time.Sleep(time.Duration(3) * time.Second) // sleep 3 seconds to wait until last offset to be flushed to zk
	targetTopicValue, err := zkqClient.GetTopicData(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if expectedLastOffset != targetTopicValue.LastOffset() {
		t.Errorf("Expected offset is %d, but got %d from zk", expectedLastOffset, targetTopicValue.LastOffset())
	}
}
