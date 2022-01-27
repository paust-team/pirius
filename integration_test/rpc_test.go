package integration_test

import (
	"fmt"
	"testing"
)

func TestHeartBeat(t *testing.T) {
	testContext := DefaultShapleQTestContext(t)
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
	testContext := DefaultShapleQTestContext(t)
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	// test body
	testParams := testContext.TestParams()

	err := adminClient.CreateTopic(testParams.topics[0], testParams.topicDescriptions[0])
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(testParams.topics[0])
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
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	// test body
	testParams := testContext.TestParams()

	err := adminClient.CreateTopic(testParams.topics[0], testParams.topicDescriptions[0])
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(testParams.topics[0])
	if err != nil {
		t.Fatal(err)
		return
	}

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	if err = adminClient.DeleteFragment(testParams.topics[0], fragment.Id); err != nil {
		t.Fatal(err)
	}

	// TODO:: get list of fragments and check whether the fragment is deleted

	if err = adminClient.DeleteTopic(testParams.topics[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := adminClient.DescribeTopic(testParams.topics[0]); err == nil {
		t.Fatal(err)
	}
}

func TestDescribeFragment(t *testing.T) {
	testContext := DefaultShapleQTestContext(t)
	testContext.RunBrokers()
	defer testContext.Terminate()

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	// test body
	testParams := testContext.TestParams()

	err := adminClient.CreateTopic(testParams.topics[0], testParams.topicDescriptions[0])
	if err != nil {
		t.Fatal(err)
		return
	}

	fragment, err := adminClient.CreateFragment(testParams.topics[0])
	if err != nil {
		t.Fatal(err)
		return
	}
	expectedFragmentId := fragment.Id

	if fragment.Id == 0 {
		t.Fatal(err)
	}

	fragment, err = adminClient.DescribeFragment(testParams.topics[0], fragment.Id)
	if err != nil {
		t.Fatal(err)
	}

	if fragment.Id != expectedFragmentId {
		t.Error("fragmentId mismatched")
	}

	topic, err := adminClient.DescribeTopic(testParams.topics[0])
	if err != nil {
		t.Fatal(err)
	}

	if len(topic.FragmentIds) != 1 {
		t.Error("invalid count of fragment")
	} else if topic.FragmentIds[0] != expectedFragmentId {
		t.Error("fragmentId mismatched in topic")
	}
}
