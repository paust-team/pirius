package integration_test

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/common"
	"sync"
	"testing"
)

func TestConnect(t *testing.T) {
	testContext := DefaultShapleQTestContext("TestConnect", t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()

	testContext.AddProducerContext(common.GenerateNodeId(), testParams.topic)
	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topic, testParams.fragmentOffsets)
}

func TestPubSub(t *testing.T) {

	testContext := DefaultShapleQTestContext("TestPubSub", t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	expectedRecords := testParams.testRecords[0]
	receivedRecords := make([][]byte, 0)

	// setup clients
	testContext.AddProducerContext(common.GenerateNodeId(), testParams.topic).
		onError(func(err error) {
			t.Error(err)
		}).
		asyncPublish(expectedRecords).
		waitFinished()

	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topic, testParams.fragmentOffsets).
		onComplete(func() {
			for _, record := range expectedRecords {
				if !contains(receivedRecords, record) {
					t.Error("Record is not exists: ", record)
				}
			}
		}).
		onError(func(err error) {
			t.Error(err)
		}).
		onSubscribe(testParams.consumerBatchSize, testParams.consumerFlushInterval, func(received *client.SubscribeResult) bool {
			receivedRecords = append(receivedRecords, received.Items[0].Data)
			fmt.Printf("received fetch result. fragmentId = %d, seq = %d, node id = %s\n", received.Items[0].FragmentId, received.Items[0].SeqNum, received.Items[0].NodeId)
			if len(receivedRecords) == len(expectedRecords) {
				return true
			} else {
				return false
			}
		}).
		waitFinished()

	Sleep(3) // sleep 3 seconds to wait until last offset to be flushed to zk
	for fragmentId := range testParams.fragmentOffsets {
		fragmentValue, err := testContext.zkClient.GetTopicFragmentData(testParams.topic, fragmentId)
		if err != nil {
			t.Fatal(err)
		}

		if uint64(len(expectedRecords)) != fragmentValue.LastOffset() {
			t.Errorf("expected last offset (%d) is not matched with (%d)", len(expectedRecords), fragmentValue.LastOffset())
		}
	}
}

func TestMultiClient(t *testing.T) {

	testContext := DefaultShapleQTestContext("TestMultiClient", t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	var expectedRecords [][]byte
	receivedRecords := make([]records, testParams.consumerCount)
	var wg sync.WaitGroup

	// setup producers
	for i := 0; i < testParams.producerCount; i++ {
		records := testParams.testRecords[i]
		expectedRecords = append(expectedRecords, records...)
		testContext.AddProducerContext(common.GenerateNodeId(), testParams.topic).
			onError(func(err error) {
				t.Error(err)
			}).
			asyncPublish(records)
	}

	// setup consumers
	for i := 0; i < testParams.consumerCount; i++ {
		wg.Add(1)
		func(index int) {
			nodeId := fmt.Sprintf("consumer%024d", index)
			testContext.AddConsumerContext(nodeId, testParams.topic, testParams.fragmentOffsets).
				onComplete(func() {
					for _, record := range receivedRecords[index] {
						if !contains(expectedRecords, record) {
							t.Errorf("Record(%s) is not exists: consumer(%s) ", record, nodeId)
						}
					}
					wg.Done()
				}).
				onError(func(err error) {
					t.Error(err)
					wg.Done()
				}).
				onSubscribe(testParams.consumerBatchSize, testParams.consumerFlushInterval, func(received *client.SubscribeResult) bool {
					receivedRecords[index] = append(receivedRecords[index], received.Items[0].Data)
					if len(receivedRecords[index]) == len(expectedRecords) {
						fmt.Printf("consumer(%s) is finished\n", nodeId)
						return true
					} else {
						return false
					}
				})
		}(i)
	}
	wg.Wait()
}

func TestBatchedFetch(t *testing.T) {
	testContext := DefaultShapleQTestContext("TestBatchedFetch", t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	var expectedRecords [][]byte
	receivedRecords := make([][]byte, 0)

	// setup producers
	for i := 0; i < testParams.producerCount; i++ {
		records := testParams.testRecords[i]
		expectedRecords = append(expectedRecords, records...)
		testContext.AddProducerContext(common.GenerateNodeId(), testParams.topic).
			onError(func(err error) {
				t.Error(err)
			}).
			asyncPublish(records)
	}

	// setup consumer
	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topic, testParams.fragmentOffsets).
		onComplete(func() {
			for _, record := range receivedRecords {
				if !contains(expectedRecords, record) {
					t.Errorf("Record(%s) is not exists", record)
				}
			}
		}).
		onError(func(err error) {
			t.Error(err)
		}).
		onSubscribe(testParams.consumerBatchSize, testParams.consumerFlushInterval, func(received *client.SubscribeResult) bool {
			for _, data := range received.Items {
				receivedRecords = append(receivedRecords, data.Data)
			}
			if len(receivedRecords) == len(expectedRecords) {
				fmt.Println("consumer is finished")
				return true
			} else {
				return false
			}
		}).
		waitFinished()
}
