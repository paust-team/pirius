package coordinator_helper

import (
	"github.com/paust-team/shapleq/coordinator-helper/helper"
	logger "github.com/paust-team/shapleq/log"
	"testing"
	"time"
)

func TestCoordinatorWrapper_IncreaseLastOffset(t *testing.T) {
	testClient := NewCoordinatorWrapper([]string{"127.0.0.1"}, 3000, 2000, logger.NewQLogger("coordinator-test", logger.Info))
	if err := testClient.Connect(); err != nil {
		t.Fatal(err)
	}
	if err := testClient.CreatePathsIfNotExist(); err != nil {
		t.Fatal(err)
	}
	defer testClient.Close()
	defer testClient.RemoveAllPath()

	expectedTopic := "topic-test-coordi"
	expectedTopicDescription := "topicMeta-test-coordi"
	var initialLastOffset uint64 = 1
	var expectedLastOffset uint64 = 2
	var expectedFragmentId uint32 = 1

	topicValue := helper.NewFrameForTopicFromValues(expectedTopicDescription, 1, 1, 1)

	if err := testClient.AddTopicFrame(expectedTopic, topicValue); err != nil {
		t.Fatal(err)
	}

	topicFragmentValue := helper.NewFrameForFragmentFromValues(initialLastOffset, 1)
	if err := testClient.AddTopicFragmentFrame(expectedTopic, expectedFragmentId, topicFragmentValue); err != nil {
		t.Fatal(err)
	}

	topics, err := testClient.GetTopicFrames()
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
