package helper

import (
	"errors"
	"github.com/paust-team/shapleq/pqerror"
	"testing"
)

type topicManagingTestClient struct {
	*TopicManagingHelper
	*FragmentManagingHelper
	*BootstrappingHelper
}

func newTopicManagingTestClient() *topicManagingTestClient {
	return &topicManagingTestClient{
		TopicManagingHelper:    NewTopicManagerHelper(client.Coordinator),
		FragmentManagingHelper: NewFragmentManagingHelper(client.Coordinator),
		BootstrappingHelper:    NewBootstrappingHelper(client.Coordinator),
	}
}

func TestMakeTopicData(t *testing.T) {
	var expectedDescription = "test topic meta"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 0
	var expectedNumPublishers uint64 = 1

	data := NewFrameForTopicFromValues(expectedDescription, expectedNumFragments, expectedReplicationFactor, expectedNumPublishers)

	if data.Description() != expectedDescription {
		t.Error("Description is not equal")
	}

	if data.NumFragments() != expectedNumFragments {
		t.Error("Num Fragment is not equal")
	}

	if data.ReplicationFactor() != expectedReplicationFactor {
		t.Error("Replication Factor is not equal")
	}

	if data.NumPublishers() != expectedNumPublishers {
		t.Error("NumPublisher is not equal")
	}
}

func TestMakeTopicFragmentData(t *testing.T) {
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	data := NewFrameForFragmentFromValues(expectedLastOffset, expectedNumSubscribers)

	if data.LastOffset() != expectedLastOffset {
		t.Error("LastOffset is not equal")
	}

	if data.NumSubscribers() != expectedNumSubscribers {
		t.Error("NumSubscribers is not equal")
	}
}

func TestTopicManagingHelper_AddTopic(t *testing.T) {
	testClient := newTopicManagingTestClient()
	expectedTopic := "topic1"
	expectedTopicDescription := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicDescription, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)

	if err := testClient.AddTopicFrame(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	targetTopicValue, err := testClient.GetTopicFrame(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if targetTopicValue == nil {
		t.Fatal("failed to add topic", expectedTopic)
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

func TestTopicManagingHelper_RemoveTopic(t *testing.T) {
	testClient := newTopicManagingTestClient()
	topic := "topic5"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicMeta, expectedNumFragments, expectedReplicationFactor,
		expectedNumPublishers)
	if err := testClient.AddTopicFrame(topic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(1, 1)
	if err := testClient.AddTopicFragmentFrame(topic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	if err := testClient.AddBrokerForTopic(topic, expectedFragmentId, "127.0.0.1"); err != nil {
		var e pqerror.ZKTargetAlreadyExistsError
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
	}

	if err := testClient.RemoveTopicFrame(topic); err != nil {
		t.Fatal(err)
	}

	topics, err := testClient.GetTopicFrames()
	if err != nil {
		t.Fatal(err)
	}

	for _, top := range topics {
		if top == topic {
			t.Error("topic is not deleted")
		}
	}
}
