package helper

import (
	"strconv"
	"testing"
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
	if err := testClient.AddTopicFrame(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := testClient.AddTopicFragmentFrame(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
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
	if err := testClient.AddTopicFrame(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := NewFrameForFragmentFromValues(expectedLastOffset, expectedNumSubscribers)
	if err := testClient.AddTopicFragmentFrame(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	if err := testClient.RemoveTopicFragmentFrame(expectedTopic, expectedFragmentId); err != nil {
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
