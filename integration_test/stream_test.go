package integration_test

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/common"
	"log"
	"os"
	"sync"
	"testing"
)

func getRecordsFromFile(fileName string) [][]byte {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var records [][]byte
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		data := []byte(scanner.Text())
		records = append(records, data)
	}
	return records
}

func contains(s [][]byte, e []byte) bool {
	for _, a := range s {
		if bytes.Compare(a, e) == 0 {
			return true
		}
	}
	return false
}

func TestConnect(t *testing.T) {
	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	// setup topic and fragments
	nodeId := common.GenerateNodeId()
	topic := "topic1"
	fragmentCount := 1
	fragmentOffsets := testContext.SetupTopicAndFragments(topic, fragmentCount)

	// test connect
	testContext.AddProducerContext(nodeId, topic)
	testContext.AddConsumerContext(nodeId, topic, fragmentOffsets)
}

func TestPubSub(t *testing.T) {

	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	// setup topic and fragments
	topic := "topic2"
	fragmentCount := 1
	fragmentOffsets := testContext.SetupTopicAndFragments(topic, fragmentCount)

	expectedRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	nodeId := common.GenerateNodeId()
	actualRecords := make([][]byte, 0)

	// setup clients
	testContext.AddProducerContext(nodeId, topic).
		asyncPublish(expectedRecords).
		waitFinished()

	testContext.AddConsumerContext(nodeId, topic, fragmentOffsets).
		onSubscribe(1, 0, func(received *client.SubscribeResult) bool {
			actualRecords = append(actualRecords, received.Items[0].Data)
			fmt.Printf("received fetch result. fragmentId = %d, seq = %d, node id = %s\n", received.Items[0].FragmentId, received.Items[0].SeqNum, received.Items[0].NodeId)
			if len(actualRecords) == len(expectedRecords) {
				return true
			} else {
				return false
			}
		}).
		onComplete(func() {
			for _, record := range expectedRecords {
				if !contains(actualRecords, record) {
					t.Error("Record is not exists: ", record)
				}
			}
			Sleep(3) // sleep 3 seconds to wait until last offset to be flushed to zk
			for fragmentId := range fragmentOffsets {
				fragmentValue, err := testContext.zkClient.GetTopicFragmentData(topic, fragmentId)
				if err != nil {
					t.Fatal(err)
				}

				if uint64(len(expectedRecords)) != fragmentValue.LastOffset() {
					t.Errorf("expected last offset (%d) is not matched with (%d)", len(expectedRecords), fragmentValue.LastOffset())
				}
			}
		}).
		waitFinished()
}

func TestMultiClient(t *testing.T) {

	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	// setup topic and fragments
	topic := "topic3"
	fragmentCount := 1
	fragmentOffsets := testContext.SetupTopicAndFragments(topic, fragmentCount)

	// setup clients
	var totalPublishedRecords [][]byte
	var wg sync.WaitGroup

	// setup 3 producers
	records1 := getRecordsFromFile("data1.txt")
	records2 := getRecordsFromFile("data2.txt")
	records3 := getRecordsFromFile("data3.txt")

	totalPublishedRecords = append(totalPublishedRecords, records1...)
	totalPublishedRecords = append(totalPublishedRecords, records2...)
	totalPublishedRecords = append(totalPublishedRecords, records3...)

	testContext.AddProducerContext(common.GenerateNodeId(), topic).
		asyncPublish(records1)
	testContext.AddProducerContext(common.GenerateNodeId(), topic).
		asyncPublish(records2)
	testContext.AddProducerContext(common.GenerateNodeId(), topic).
		asyncPublish(records3)

	// setup n consumers
	var consumerCount = 5
	type SubscribedRecords [][]byte
	totalSubscribedRecords := make([]SubscribedRecords, consumerCount)

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		func(index int) {
			nodeId := fmt.Sprintf("consumer%024d", index)
			testContext.AddConsumerContext(nodeId, topic, fragmentOffsets).
				onSubscribe(1, 0, func(received *client.SubscribeResult) bool {
					totalSubscribedRecords[index] = append(totalSubscribedRecords[index], received.Items[0].Data)
					if len(totalSubscribedRecords[index]) == len(totalPublishedRecords) {
						fmt.Printf("consumer(%s) is finished\n", nodeId)
						return true
					} else {
						return false
					}
				}).
				onComplete(func() {
					wg.Done()
					for _, record := range totalSubscribedRecords[index] {
						if !contains(totalPublishedRecords, record) {
							t.Errorf("Record(%s) is not exists: consumer(%s) ", record, nodeId)
						}
					}
				})
		}(i)
	}
	wg.Wait()
}

func TestBatchedFetch(t *testing.T) {
	testContext := DefaultShapleQTestContext(t)
	testContext.Start()
	defer testContext.Stop()

	// setup topic and fragments
	topic := "topic4"
	fragmentCount := 1
	fragmentOffsets := testContext.SetupTopicAndFragments(topic, fragmentCount)
	nodeId := common.GenerateNodeId()
	// setup clients
	var totalPublishedRecords [][]byte

	// setup producer
	records1 := getRecordsFromFile("data1.txt")
	totalPublishedRecords = append(totalPublishedRecords, records1...)

	testContext.AddProducerContext(nodeId, topic).
		asyncPublish(records1)

	// setup consumer with batch size and flush interval
	var maxBatchSize uint32 = 32
	var flushInterval uint32 = 200
	actualRecords := make([][]byte, 0)

	testContext.AddConsumerContext(nodeId, topic, fragmentOffsets).
		onSubscribe(maxBatchSize, flushInterval, func(received *client.SubscribeResult) bool {
			for _, data := range received.Items {
				actualRecords = append(actualRecords, data.Data)
			}

			if len(actualRecords) == len(totalPublishedRecords) {
				fmt.Printf("consumer(%s) is finished\n", nodeId)
				return true
			} else {
				return false
			}
		}).
		onComplete(func() {
			for _, record := range actualRecords {
				if !contains(totalPublishedRecords, record) {
					t.Errorf("Record(%s) is not exists: consumer(%s) ", record, nodeId)
				}
			}
		}).
		waitFinished()
}