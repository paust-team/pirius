package helper

import (
	"errors"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	"testing"
)

type bootstrappingTestClient struct {
	*BootstrappingHelper
	*TopicManagingHelper
	*FragmentManagingHelper
}

func newBootstrappingTestClient() *bootstrappingTestClient {
	return &bootstrappingTestClient{
		BootstrappingHelper:    NewBootstrappingHelper(client.Coordinator),
		TopicManagingHelper:    NewTopicManagerHelper(client.Coordinator),
		FragmentManagingHelper: NewFragmentManagingHelper(client.Coordinator),
	}
}

func TestBootstrappingHelper_AddBroker(t *testing.T) {
	testClient := newBootstrappingTestClient()

	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"
	if err = testClient.AddBroker(brokerAddr); err != nil {
		t.Fatal(err)
	}

	brokers, err := testClient.GetBrokers()
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

func TestBootstrappingHelper_RemoveBroker(t *testing.T) {
	testClient := newBootstrappingTestClient()
	broker := "127.0.0.1:1101"
	if err := testClient.AddBroker(broker); err != nil {
		t.Fatal(err)
	}

	if err := testClient.RemoveBroker(broker); err != nil {
		t.Fatal(err)
	}

	brokers, err := testClient.GetBrokers()
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range brokers {
		if b == broker {
			t.Error("broker did not deleted")
		}
	}
}

func TestBootstrappingHelper_AddBrokerForTopic(t *testing.T) {
	testClient := newBootstrappingTestClient()

	expectedTopic := "topic3"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	if err := testClient.AddTopicFrame(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := testClient.AddTopicFragmentFrame(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	host, err := network.GetOutboundIP()
	if err != nil {
		t.Fatal(err)
	}

	brokerAddr := host.String() + ":1101"
	err = testClient.AddBrokerForTopic(expectedTopic, expectedFragmentId, brokerAddr)
	if err != nil {
		t.Fatal(err)
	}

	brokers, err := testClient.GetBrokersOfTopic(expectedTopic, expectedFragmentId)
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
	testClient := newBootstrappingTestClient()

	topic := "topic4"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	err := testClient.AddTopicFrame(topic, topicValue)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := testClient.GetTopicFrames()
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
	topicFragmentValue := NewFrameForFragmentFromValues(0, 1)
	if err := testClient.AddTopicFragmentFrame(topic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	if err := testClient.AddBrokerForTopic(topic, expectedFragmentId, brokerAddr); err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
	}

	if err := testClient.RemoveBrokerOfTopic(topic, expectedFragmentId, brokerAddr); err != nil {
		t.Fatal(err)
	}

	brokers, err := testClient.GetBrokersOfTopic(topic, expectedFragmentId)
	if err != nil {
		t.Fatal(err)
	}

	for _, broker := range brokers {
		if broker == host.String() {
			t.Error("topic broker did not deleted")
		}
	}
}
