package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/paust-team/paustq/broker"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/client/consumer"
	"github.com/paust-team/paustq/client/producer"
	paustqproto "github.com/paust-team/paustq/proto"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestClient_Connect(t *testing.T) {

	ip := "127.0.0.1"
	port := 9000
	host := fmt.Sprintf("%s:%d", ip, port)
	ctx := context.Background()
	topic := "test_topic1"

	// Start broker
	brokerInstance := broker.NewBroker(uint16(port))
	go brokerInstance.Start()
	defer brokerInstance.Clean()
	defer brokerInstance.Stop()

	time.Sleep(1 * time.Second)

	// Start client
	c := client.NewStreamClient(ctx, host, paustqproto.SessionType_ADMIN)

	if err := c.ConnectWithTopic(topic); err != nil {
		t.Error("Error on connect. ", err)
		return
	}

	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func TestPubSub(t *testing.T) {

	ip := "127.0.0.1"
	port := 9001

	testRecordMap := map[string][][]byte{
		"topic1": {
			{'g', 'o', 'o', 'g', 'l', 'e'},
			{'p', 'a', 'u', 's', 't', 'q'},
			{'1', '2', '3', '4', '5', '6'}},
	}
	topic := "topic1"
	receivedRecordMap := make(map[string][][]byte)

	host := fmt.Sprintf("%s:%d", ip, port)
	ctx1 := context.Background()
	ctx2 := context.Background()

	// Start broker
	brokerInstance := broker.NewBroker(uint16(port))
	go brokerInstance.Start()
	defer brokerInstance.Clean()
	defer brokerInstance.Stop()

	time.Sleep(1 * time.Second)

	// Start producer
	producerClient := producer.NewProducer(ctx1, host)
	if producerClient.Connect(topic) != nil {
		t.Error("Error on connect")
	}

	for _, record := range testRecordMap[topic] {
		producerClient.Publish(record)
	}

	producerClient.WaitAllPublishResponse()

	if err := producerClient.Close(); err != nil {
		t.Error(err)
	}

	// Start consumer
	consumerClient := consumer.NewConsumer(ctx2, host, consumer.NewEndSubscriptionCondition().OnReachEnd())
	if consumerClient.Connect(topic) != nil {
		t.Error("Error on connect")
		return
	}

	for response := range consumerClient.Subscribe(0) {
		if response.Error != nil {
			t.Error(response.Error)
		} else {
			receivedRecordMap[topic] = append(receivedRecordMap[topic], response.Data)
		}
	}

	expectedResults := testRecordMap[topic]
	receivedResults := receivedRecordMap[topic]

	if len(expectedResults) != len(receivedResults) {
		t.Error("Length Mismatch - Expected records: ", len(expectedResults), ", Received records: ", len(receivedResults))
	}
	for i, record := range expectedResults {
		if bytes.Compare(receivedResults[i], record) != 0 {
			t.Error("Record is not same")
		}
	}

	if err := consumerClient.Close(); err != nil {
		t.Error(err)
	}
}

func TestPubsub_Chunk(t *testing.T) {

	ip := "127.0.0.1"
	port := 9002
	var chunkSize uint32 = 1024

	topic := "topic2"

	host := fmt.Sprintf("%s:%d", ip, port)
	ctx1 := context.Background()
	ctx2 := context.Background()

	// Start broker
	brokerInstance := broker.NewBroker(uint16(port))
	go brokerInstance.Start()
	defer brokerInstance.Clean()
	defer brokerInstance.Stop()

	time.Sleep(1 * time.Second)

	// Start producer
	producerClient := producer.NewProducer(ctx1, host).WithChunkSize(chunkSize)
	if producerClient.Connect(topic) != nil {
		t.Error("Error on connect")
	}

	data, err := ioutil.ReadFile("sample.txt")

	if err != nil {
		t.Error(err)
		return
	}

	producerClient.Publish(data)
	producerClient.WaitAllPublishResponse()

	if err := producerClient.Close(); err != nil {
		t.Error(err)
	}

	expectedLen := len(data)
	// Start consumer

	consumerClient := consumer.NewConsumer(ctx2, host, consumer.NewEndSubscriptionCondition().OnReachEnd())
	if consumerClient.Connect(topic) != nil {
		t.Error("Error on connect")
		return
	}

	receivedLen := 0
	for response := range consumerClient.Subscribe(0) {
		if response.Error != nil {
			t.Error(response.Error)
		} else {
			receivedLen = len(response.Data)
		}
	}

	if err := consumerClient.Close(); err != nil {
		t.Error(err)
	}

	if expectedLen != receivedLen {
		t.Error("Length Mismatch - Expected record length: ", expectedLen, ", Received record length: ", receivedLen)
	}
}

func readFromFileLineBy(fileName string) (int, [][]byte) {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var records [][]byte
	count := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		data := []byte(scanner.Text())
		records = append(records, data)
		count++
	}
	return count, records
}

func TestMultiClient(t *testing.T) {

	ip := "127.0.0.1"
	port := 9003
	var chunkSize uint32 = 1024

	topic := "topic3"

	host := fmt.Sprintf("%s:%d", ip, port)

	// Start broker
	brokerInstance := broker.NewBroker(uint16(port))
	go brokerInstance.Start()
	defer brokerInstance.Clean()
	defer brokerInstance.Stop()

	time.Sleep(1 * time.Second)

	ctxP1 := context.Background()
	ctxP2 := context.Background()
	ctxP3 := context.Background()

	ctxC1 := context.Background()
	ctxC2 := context.Background()

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	// Start producer
	var expectedSentCount int
	var sentRecords [][]byte
	runProducer := func(ctx context.Context, fileName string) {
		count, sendingRecords := readFromFileLineBy(fileName)
		expectedSentCount += count
		go func() {
			defer wg.Done()

			producerClient := producer.NewProducer(ctx, host).WithChunkSize(chunkSize)
			if producerClient.Connect(topic) != nil {
				t.Error("Error on connect")
			}

			for _, record := range sendingRecords {
				producerClient.Publish(record)
			}

			mu.Lock()
			sentRecords = append(sentRecords, sendingRecords...)
			mu.Unlock()

			producerClient.WaitAllPublishResponse()

			if err := producerClient.Close(); err != nil {
				t.Error(err)
			}
			fmt.Println("publish done with file :", fileName, "sent total:", len(sendingRecords))
		}()
	}

	wg.Add(3)
	runProducer(ctxP1, "data1.txt")
	runProducer(ctxP2, "data2.txt")
	runProducer(ctxP3, "data3.txt")

	// Start consumer
	var receivedRecords1 [][]byte
	var receivedRecords2 [][]byte

	runConsumer := func(ctx context.Context, receivedRecords *[][]byte) {
		go func() {
			defer wg.Done()

			consumerClient := consumer.NewConsumer(ctx, host, consumer.NewEndSubscriptionCondition().Eternal())
			if consumerClient.Connect(topic) != nil {
				t.Error("Error on connect")
				return
			}

			receiveCount := 0
			for response := range consumerClient.Subscribe(0) {
				if response.Error != nil {
					t.Error(response.Error)
				} else {

					*receivedRecords = append(*receivedRecords, response.Data)
					receiveCount++
					if expectedSentCount == receiveCount {
						fmt.Println("complete consumer. received :", receiveCount)
						break
					}
				}
			}

			if err := consumerClient.Close(); err != nil {
				t.Error(err)
			}
		}()

	}

	wg.Add(2)
	go runConsumer(ctxC1, &receivedRecords1)
	go runConsumer(ctxC2, &receivedRecords2)

	wg.Wait()

	checkConsumerResults := func(receivedRecords [][]byte) {
		if len(sentRecords) != len(receivedRecords) {
			t.Error("Length Mismatch - Expected records: ", len(sentRecords), ", Received records: ", len(receivedRecords))
		}

		for _, record := range receivedRecords {
			if !contains(sentRecords, record) {
				t.Error("Record is not exists: ", record)
			}
		}
	}

	checkConsumerResults(receivedRecords1)
	checkConsumerResults(receivedRecords2)
}

func contains(s [][]byte, e []byte) bool {
	for _, a := range s {
		if bytes.Compare(a, e) == 0 {
			return true
		}
	}
	return false
}
