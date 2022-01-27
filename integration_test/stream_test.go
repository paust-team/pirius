package integration_test

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/pqerror"
	"sync"
	"testing"
)

func TestConnect(t *testing.T) {
	testContext := DefaultShapleQTestContext(t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()

	testContext.AddProducerContext(common.GenerateNodeId(), testParams.topics[0])
	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topicTargets)
}

func TestPubSub(t *testing.T) {

	testContext := DefaultShapleQTestContext(t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	expectedRecords := testParams.testRecords[0]
	receivedRecords := make([][]byte, 0)

	// setup clients
	testContext.AddProducerContext(common.GenerateNodeId(), testParams.topics[0]).
		onError(func(err error) {
			t.Error(err)
		}).
		asyncPublish(expectedRecords).
		waitFinished()

	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topicTargets).
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

	adminClient := testContext.CreateAdminClient()
	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	for _, fragmentId := range testParams.topicTargets[0].FragmentIds() {
		fragment, err := adminClient.DescribeFragment(testParams.topicTargets[0].Topic(), fragmentId)
		if err != nil {
			t.Fatal(err)
		}

		if uint64(len(expectedRecords)) != fragment.LastOffset {
			t.Errorf("expected last offset (%d) is not matched with (%d)", len(expectedRecords), fragment.LastOffset)
		}
	}
}

func TestMultiClient(t *testing.T) {

	testContext := DefaultShapleQTestContext(t).
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
		testContext.AddProducerContext(common.GenerateNodeId(), testParams.topics[0]).
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
			testContext.AddConsumerContext(nodeId, testParams.topicTargets).
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
	testContext := DefaultShapleQTestContext(t).
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
		testContext.AddProducerContext(common.GenerateNodeId(), testParams.topics[0]).
			onError(func(err error) {
				t.Error(err)
			}).
			asyncPublish(records)
	}

	// setup consumer
	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topicTargets).
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

func TestMultiFragmentsTotalConsume(t *testing.T) {

	testContext := DefaultShapleQTestContext(t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	var expectedRecords [][]byte = testParams.testRecords[0]
	receivedRecords := make([][]byte, 0)

	// setup producer
	testContext.AddProducerContext(common.GenerateNodeId(), testParams.topics[0]).
		onError(func(err error) {
			t.Error(err)
		}).
		asyncPublish(expectedRecords)

	// setup consumer
	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topicTargets).
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
			receivedRecords = append(receivedRecords, received.Items[0].Data)
			if len(receivedRecords) == len(expectedRecords) {
				fmt.Println("consumer is finished")
				return true
			} else {
				return false
			}
		}).
		waitFinished()
}

func TestMultiFragmentsOptionalConsume(t *testing.T) {
	testContext := DefaultShapleQTestContext(t)
	testContext.
		WithBrokerTimeout(1500).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	var expectedRecords [][]byte = testParams.testRecords[0]
	receivedRecords := map[uint32]records{}
	var wg sync.WaitGroup
	var mu sync.Mutex

	// setup producer
	testContext.AddProducerContext(common.GenerateNodeId(), testParams.topics[0]).
		onError(func(err error) {
			t.Error(err)
		}).
		asyncPublish(expectedRecords)

	// setup consumer for each fragment
	for fragmentId, offset := range testParams.topicTargets[0].FragmentOffsets() {
		wg.Add(1)
		fid := fragmentId
		startOffset := offset
		nodeId := fmt.Sprintf("consumer%024d", fid)
		receivedRecordsForFragments := records{}
		topicTarget := common.NewTopicFragmentsWithOffset(testParams.topicTargets[0].Topic(), common.FragmentOffsetMap{fid: startOffset})
		testContext.AddConsumerContext(nodeId, []*common.TopicFragments{topicTarget}).
			onSubscribe(testParams.consumerBatchSize, testParams.consumerFlushInterval, func(received *client.SubscribeResult) bool {
				receivedRecordsForFragments = append(receivedRecordsForFragments, received.Items[0].Data)
				return false
			}).
			onError(func(err error) {
				// escape when timed out
				if _, ok := err.(pqerror.SocketClosedError); ok {
					fmt.Printf("consumer(%s) for fragment(%d) is finished from timeout\n", nodeId, fid)
					for _, record := range receivedRecordsForFragments {
						if !contains(expectedRecords, record) {
							t.Errorf("Record(%s) is not exists: consumer(%s) for fragment(%d) ", record, nodeId, fid)
						}
					}
					mu.Lock()
					receivedRecords[fid] = receivedRecordsForFragments
					mu.Unlock()
				}
				wg.Done()
			})
	}

	wg.Wait()
	// check total received records count
	totalCount := 0
	for _, record := range receivedRecords {
		totalCount += len(record)
	}

	if totalCount != len(expectedRecords) {
		t.Errorf("Published %d data but received %d data", len(expectedRecords), totalCount)
	}
}

func TestMultiTopic(t *testing.T) {

	testContext := DefaultShapleQTestContext(t).
		RunBrokers().
		SetupTopics()
	defer testContext.Terminate()

	// test body
	testParams := testContext.TestParams()
	var expectedRecords [][]byte
	receivedRecords := make([][]byte, 0)

	// setup clients from topic
	for i, topic := range testParams.topics {
		// setup a producer per topic
		records := testParams.testRecords[i]
		expectedRecords = append(expectedRecords, records...)
		testContext.AddProducerContext(common.GenerateNodeId(), topic).
			onError(func(err error) {
				t.Error(err)
			}).
			asyncPublish(records)
	}

	// setup consumer
	testContext.AddConsumerContext(common.GenerateNodeId(), testParams.topicTargets).
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
