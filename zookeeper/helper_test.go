package zookeeper

import (
	"errors"
	"fmt"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	"os"
	"strconv"
	"testing"
	"time"
)

var zkqClient *ZKQClient

func TestMain(m *testing.M) {
	zkqClient = NewZKQClient([]string{"127.0.0.1"}, 3000, 2000)
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

// brokers
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

// topics
func TestZKClient_AddTopic(t *testing.T) {
	expectedTopic := "topic1"
	expectedTopicDescription := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicDescription, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)

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

	if targetTopicValue.NumFragments() != expectedNumFragments {
		t.Fatal("num fragments not matched ", expectedNumFragments, targetTopicValue.NumFragments())
	}

	if targetTopicValue.ReplicationFactor() != expectedReplicationFactor {
		t.Fatal("replication factor not matched ", expectedReplicationFactor, targetTopicValue.ReplicationFactor())
	}

	if targetTopicValue.NumPublishers() != expectedNumPublishers {
		t.Fatal("num publisher not matched ", expectedNumPublishers, targetTopicValue.NumPublishers())
	}
}

func TestZKClient_RemoveTopic(t *testing.T) {
	topic := "topic5"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicMeta, expectedNumFragments, expectedReplicationFactor,
		expectedNumPublishers)
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

// topic fragments
func TestZKClient_AddTopicFragment(t *testing.T) {
	expectedTopic := "topic7"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := common.NewFragmentDataFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := zkqClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	fragments, err := zkqClient.GetTopicFragments(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if fragments[len(fragments)-1] != strconv.Itoa(int(expectedFragmentId)) {
		t.Fatal("failed to add topic fragment ")
	}
}

func TestZKClient_RemoveTopicFragment(t *testing.T) {
	expectedTopic := "topic8"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := common.NewFragmentDataFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := zkqClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	if err := zkqClient.RemoveTopicFragment(expectedTopic, expectedFragmentId); err != nil {
		t.Fatal(err)
	}

	fragments, err := zkqClient.GetTopicFragments(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	for _, fragmentId := range fragments {
		if fragmentId == strconv.Itoa(int(expectedFragmentId)) {
			t.Error("topic fragment did not deleted")
		}
	}
}

// topic fragment brokers
func TestZKClient_AddTopicFragmentBroker(t *testing.T) {
	expectedTopic := "topic3"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := common.NewFragmentDataFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := zkqClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"
	err = zkqClient.AddBrokerForTopic(expectedTopic, expectedFragmentId, brokerAddr)
	if err != nil {
		t.Fatal(err)
	}

	brokers, err := zkqClient.GetBrokersOfTopic(expectedTopic, expectedFragmentId)
	if err != nil {
		t.Fatal(err)
	}

	if len(brokers) == 0 {
		t.Fatal("no brokers")
	}
	if brokers[len(brokers)-1] != brokerAddr {
		t.Fatal("failed to add topic broker ", host)
	}
}

func TestZKClient_RemoveTopicFragmentBroker(t *testing.T) {
	topic := "topic4"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
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
	topicFragmentValue := common.NewFragmentDataFromValues(0, 1)
	if err := zkqClient.AddTopicFragment(topic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	if err := zkqClient.AddBrokerForTopic(topic, expectedFragmentId, brokerAddr); err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
	}

	if err := zkqClient.RemoveBrokerOfTopic(topic, expectedFragmentId, brokerAddr); err != nil {
		t.Fatal(err)
	}

	brokers, err := zkqClient.GetBrokersOfTopic(topic, expectedFragmentId)
	if err != nil {
		t.Fatal(err)
	}

	for _, broker := range brokers {
		if broker == host.String() {
			t.Error("topic broker did not deleted")
		}
	}
}

// etc
func TestZKQClient_IncreaseLastOffset(t *testing.T) {
	expectedTopic := "topic6"
	expectedTopicDescription := "topicMeta6"
	var initialLastOffset uint64 = 1
	var expectedLastOffset uint64 = 2
	var expectedFragmentId uint32 = 1

	topicValue := common.NewTopicDataFromValues(expectedTopicDescription, 1, 1, 1)

	if err := zkqClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := common.NewFragmentDataFromValues(initialLastOffset, 1)
	if err := zkqClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	topics, err := zkqClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("no topics")
	}

	offset, err := zkqClient.IncreaseLastOffset(expectedTopic, expectedFragmentId)
	if expectedLastOffset != offset {
		t.Errorf("Expected offset is %d, but got %d from memory", expectedLastOffset, offset)
	}

	time.Sleep(time.Duration(3) * time.Second) // sleep 3 seconds to wait until last offset to be flushed to zk
	targetTopicFragmentValue, err := zkqClient.GetTopicFragmentData(expectedTopic, expectedFragmentId)
	if err != nil {
		t.Fatal(err)
	}

	if expectedLastOffset != targetTopicFragmentValue.LastOffset() {
		t.Errorf("Expected offset is %d, but got %d from zk", expectedLastOffset, targetTopicFragmentValue.LastOffset())
	}
}
