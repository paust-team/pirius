package integration_test

import (
	"fmt"
	"testing"
)

func TestHeartBeat(t *testing.T) {
	testContext := DefaultShapleQTestContext("TestHeartBeat", t)
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	pongMsg, err := adminClient.Heartbeat("test", 1)
	if err != nil {
		t.Error(pongMsg)
		return
	}
	fmt.Println(pongMsg.Echo)
}

func TestCreateTopicAndFragment(t *testing.T) {
	testContext := DefaultShapleQTestContext("TestCreateTopicAndFragment", t)
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	// test body
	testParams := testContext.TestParams()

	err := adminClient.CreateTopic(testParams.topic, testParams.topicDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(testParams.topic)
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
	testContext := DefaultShapleQTestContext("TestDeleteTopicAndFragment", t)
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	// test body
	testParams := testContext.TestParams()

	err := adminClient.CreateTopic(testParams.topic, testParams.topicDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(testParams.topic)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	if err = adminClient.DeleteFragment(testParams.topic, fragment.Id); err != nil {
		t.Fatal(err)
	}

	// TODO:: get list of fragments and check whether the fragment is deleted

	if err = adminClient.DeleteTopic(testParams.topic); err != nil {
		t.Fatal(err)
	}
	if _, err := adminClient.DescribeTopic(testParams.topic); err == nil {
		t.Fatal(err)
	}
}

func TestDescribeFragment(t *testing.T) {
	testContext := DefaultShapleQTestContext("TestDescribeFragment", t)
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	// test body
	testParams := testContext.TestParams()

	err := adminClient.CreateTopic(testParams.topic, testParams.topicDescription)
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(testParams.topic)
	if err != nil {
		t.Fatal(err)
		return
	}
	expectedFragmentId := fragment.Id

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	fragment, err = adminClient.DescribeFragment(testParams.topic, fragment.Id)
	if err != nil {
		t.Fatal(err)
	}

	if fragment.Id != expectedFragmentId {
		t.Error("fragmentId mismatched")
	}

	topic, err := adminClient.DescribeTopic(testParams.topic)
	if err != nil {
		t.Fatal(err)
	}

	if len(topic.FragmentIds) != 1 {
		t.Error("invalid count of fragment")
	} else if topic.FragmentIds[0] != expectedFragmentId {
		t.Error("fragmentId mismatched in topic")
	}
}
