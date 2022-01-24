package integration_test

import (
	"fmt"
	"testing"
)

func TestHeartBeat(t *testing.T) {

	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	pongMsg, err := testContext.adminClient.Heartbeat("test", 1)
	if err != nil {
		t.Error(pongMsg)
		return
	}
	fmt.Println(pongMsg.Echo)
}

func TestCreateTopicAndFragment(t *testing.T) {

	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	expectedTopicName := "test-tp1"
	expectedDescription := "test-description"
	err := testContext.adminClient.CreateTopic(expectedTopicName, expectedDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := testContext.adminClient.CreateFragment(expectedTopicName)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	fmt.Println("created fragment id = ", fragment.Id)
}

func TestDeleteTopicAndFragment(t *testing.T) {

	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	expectedTopicName := "test-tp2"
	expectedDescription := "test-description"
	err := testContext.adminClient.CreateTopic(expectedTopicName, expectedDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := testContext.adminClient.CreateFragment(expectedTopicName)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	if err = testContext.adminClient.DeleteFragment(expectedTopicName, fragment.Id); err != nil {
		t.Fatal(err)
	}

	// TODO:: get list of fragments and check whether the fragment is deleted

	if err = testContext.adminClient.DeleteTopic(expectedTopicName); err != nil {
		t.Fatal(err)
	}
	if _, err := testContext.adminClient.DescribeTopic(expectedTopicName); err == nil {
		t.Fatal(err)
	}
}

func TestDescribeFragment(t *testing.T) {

	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	expectedTopicName := "test-tp3"
	expectedDescription := "test-description"
	err := testContext.adminClient.CreateTopic(expectedTopicName, expectedDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := testContext.adminClient.CreateFragment(expectedTopicName)
	if err != nil {
		t.Fatal(err)
		return
	}
	expectedFragmentId := fragment.Id

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	fragment, err = testContext.adminClient.DescribeFragment(expectedTopicName, fragment.Id)
	if err != nil {
		t.Fatal(err)
	}

	if fragment.Id != expectedFragmentId {
		t.Error("fragmentId mismatched")
	}

	topic, err := testContext.adminClient.DescribeTopic(expectedTopicName)
	if err != nil {
		t.Fatal(err)
	}

	if len(topic.FragmentIds) != 1 {
		t.Error("invalid count of fragment")
	} else if topic.FragmentIds[0] != expectedFragmentId {
		t.Error("fragmentId mismatched in topic")
	}

}
