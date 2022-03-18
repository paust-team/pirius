package helper

import (
	"strconv"
	"testing"
	"time"
)

type fragmentManagingTestClient struct {
	*TopicManagingHelper
	*FragmentManagingHelper
}

func newFragmentManagingTestClient() *fragmentManagingTestClient {
	return &fragmentManagingTestClient{
		TopicManagingHelper:    NewTopicManagerHelper(client.Coordinator),
		FragmentManagingHelper: NewFragmentManagingHelper(client.Coordinator),
	}
}

func TestZKClient_AddTopicFragment(t *testing.T) {
	testClient := newFragmentManagingTestClient()

	expectedTopic := "topic7"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	if err := testClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := testClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	fragments, err := testClient.GetTopicFragments(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	if fragments[len(fragments)-1] != strconv.Itoa(int(expectedFragmentId)) {
		t.Fatal("failed to add topic fragment ")
	}
}

func TestFragmentManagingHelper_RemoveTopicFragment(t *testing.T) {
	testClient := newFragmentManagingTestClient()

	expectedTopic := "topic8"
	expectedTopicMeta := "topicMeta1"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 1
	var expectedNumPublishers uint64 = 1
	var expectedFragmentId uint32 = 1
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicMeta, expectedNumFragments,
		expectedReplicationFactor, expectedNumPublishers)
	if err := testClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := testClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	if err := testClient.RemoveTopicFragment(expectedTopic, expectedFragmentId); err != nil {
		t.Fatal(err)
	}

	fragments, err := testClient.GetTopicFragments(expectedTopic)
	if err != nil {
		t.Fatal(err)
	}

	for _, fragmentId := range fragments {
		if fragmentId == strconv.Itoa(int(expectedFragmentId)) {
			t.Error("topic fragment did not deleted")
		}
	}
}

func TestFragmentManagingHelper_IncreaseLastOffset(t *testing.T) {
	testClient := newFragmentManagingTestClient()
	testClient.FragmentManagingHelper.StartPeriodicFlushLastOffsets(2000)

	expectedTopic := "topic6"
	expectedTopicDescription := "topicMeta6"
	var initialLastOffset uint64 = 1
	var expectedLastOffset uint64 = 2
	var expectedFragmentId uint32 = 1

	topicValue := NewFrameForTopicFromValues(expectedTopicDescription, 1, 1, 1)

	if err := testClient.AddTopic(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(initialLastOffset, 1)
	if err := testClient.AddTopicFragment(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	topics, err := testClient.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("no topics")
	}

	offset, err := testClient.IncreaseLastOffset(expectedTopic, expectedFragmentId)
	if expectedLastOffset != offset {
		t.Errorf("Expected offset is %d, but got %d from memory", expectedLastOffset, offset)
	}

	time.Sleep(time.Duration(3) * time.Second) // sleep 3 seconds to wait until last offset to be flushed to zookeeper
	targetTopicFragmentValue, err := testClient.GetTopicFragmentFrame(expectedTopic, expectedFragmentId)
	if err != nil {
		t.Fatal(err)
	}

	if expectedLastOffset != targetTopicFragmentValue.LastOffset() {
		t.Errorf("Expected offset is %d, but got %d from zookeeper", expectedLastOffset, targetTopicFragmentValue.LastOffset())
	}
}
