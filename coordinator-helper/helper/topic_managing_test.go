package helper

import (
	"testing"
)

type topicManagingTestClient struct {
	*TopicManagingHelper
}

func newTopicManagingTestClient() *topicManagingTestClient {
	return &topicManagingTestClient{
		TopicManagingHelper: NewTopicManagerHelper(client.Coordinator),
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

	topicValue := NewFrameForTopicFromValues(expectedTopicMeta, expectedNumFragments, expectedReplicationFactor,
		expectedNumPublishers)
	if err := testClient.AddTopicFrame(topic, topicValue); err != nil {
		t.Fatal(err)
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
