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
	logger "github.com/paust-team/paustq/log"
	paustqproto "github.com/paust-team/paustq/proto"
	"github.com/paust-team/paustq/zookeeper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var testLogLevel = logger.LogLevelDebug

func SleepForBroker() {
	time.Sleep(1 * time.Second)
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

func contains(s [][]byte, e []byte) bool {
	for _, a := range s {
		if bytes.Compare(a, e) == 0 {
			return true
		}
	}
	return false
}

func TestStreamClient_Connect(t *testing.T) {

	zkAddr := "127.0.0.1"

	ctx := context.Background()
	topic := "test_topic1"

	// Start broker
	brokerInstance := broker.NewBroker(zkAddr).WithLogLevel(testLogLevel)

	defer brokerInstance.Clean()
	defer SleepForBroker()

	brokerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := brokerInstance.Start(brokerCtx)
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}
	}()

	SleepForBroker()

	// Start client
	brokerHost := fmt.Sprintf("127.0.0.1:%d", brokerInstance.Port)
	c := client.NewStreamClient(brokerHost, paustqproto.SessionType_ADMIN)

	if err := c.Connect(ctx, topic); err != nil {
		t.Error(err)
		return
	}

	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func TestPubSub(t *testing.T) {

	zkAddr := "127.0.0.1"

	testRecordMap := map[string][][]byte{
		"topic1": {
			{'g', 'o', 'o', 'g', 'l', 'e'},
			{'p', 'a', 'u', 's', 't', 'q'},
			{'1', '2', '3', '4', '5', '6'}},
	}
	topic := "topic1"
	receivedRecordMap := make(map[string][][]byte)

	ctx1 := context.Background()
	ctx2 := context.Background()

	// zk client to reset
	zkClient := zookeeper.NewZKClient(zkAddr)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	// Start broker
	brokerInstance := broker.NewBroker(zkAddr).WithLogLevel(testLogLevel)

	defer brokerInstance.Clean()
	defer SleepForBroker()

	brokerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := brokerInstance.Start(brokerCtx)
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}
	}()

	SleepForBroker()

	// Create topic rpc
	rpcClient := client.NewRPCClient(zkAddr)
	if err := rpcClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer rpcClient.Close()

	rpcCtx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := rpcClient.CreateTopic(rpcCtx, topic, "", 0, 0); err != nil {
		st := status.Convert(err)
		if st.Code() != codes.AlreadyExists {
			t.Error(err)
			return
		}
	}
	// Start producer
	producerClient := producer.NewProducer(zkAddr).WithLogLevel(testLogLevel)
	if err := producerClient.Connect(ctx1, topic); err != nil {
		t.Error(err)
		return
	}

	for _, record := range testRecordMap[topic] {
		producerClient.Publish(ctx1, record)
	}

	producerClient.WaitAllPublishResponse()

	if err := producerClient.Close(); err != nil {
		t.Error(err)
	}

	// Start consumer
	consumerClient := consumer.NewConsumer(zkAddr).WithLogLevel(testLogLevel)
	if err := consumerClient.Connect(ctx2, topic); err != nil {
		t.Error(err)
		return
	}
	subscribeCh, err := consumerClient.Subscribe(ctx1, 0)
	if err != nil {
		t.Error(err)
		return
	}
	for response := range subscribeCh {
		if response.Error != nil {
			t.Error(response.Error)
		} else {
			receivedRecordMap[topic] = append(receivedRecordMap[topic], response.Data)
		}

		// break on reach end
		if response.LastOffset == response.Offset {
			break
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

	zkAddr := "127.0.0.1"
	var chunkSize uint32 = 1024

	topic := "topic2"

	ctx1 := context.Background()
	ctx2 := context.Background()

	// zk client to reset
	zkClient := zookeeper.NewZKClient(zkAddr)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	// Start broker
	brokerInstance := broker.NewBroker(zkAddr).WithLogLevel(testLogLevel)

	defer brokerInstance.Clean()
	defer SleepForBroker()

	brokerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := brokerInstance.Start(brokerCtx)
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}
	}()

	SleepForBroker()

	// Create topic rpc
	rpcClient := client.NewRPCClient(zkAddr)
	if err := rpcClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer rpcClient.Close()

	rpcCtx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := rpcClient.CreateTopic(rpcCtx, topic, "", 0, 0); err != nil {
		st := status.Convert(err)
		if st.Code() != codes.AlreadyExists {
			t.Error(err)
			return
		}
	}

	// Start producer
	producerClient := producer.NewProducer(zkAddr).WithChunkSize(chunkSize).WithLogLevel(testLogLevel)
	if err := producerClient.Connect(ctx1, topic); err != nil {
		t.Error(err)
		return
	}

	data, err := ioutil.ReadFile("sample.txt")

	if err != nil {
		t.Error(err)
		return
	}

	producerClient.Publish(ctx1, data)
	producerClient.WaitAllPublishResponse()

	if err := producerClient.Close(); err != nil {
		t.Error(err)
	}

	expectedLen := len(data)

	// Start consumer
	consumerClient := consumer.NewConsumer(zkAddr).WithLogLevel(testLogLevel)
	if err := consumerClient.Connect(ctx2, topic); err != nil {
		t.Error(err)
		return
	}

	receivedLen := 0
	subscribeCh, err := consumerClient.Subscribe(ctx2, 0)
	if err != nil {
		t.Error(err)
		return
	}
	for response := range subscribeCh {
		if response.Error != nil {
			t.Error(response.Error)
		} else {
			receivedLen = len(response.Data)
		}

		// break on reach end
		if response.LastOffset == response.Offset {
			break
		}
	}

	if err := consumerClient.Close(); err != nil {
		t.Error(err)
	}

	if expectedLen != receivedLen {
		t.Error("Length Mismatch - Expected record length: ", expectedLen, ", Received record length: ", receivedLen)
	}
}

func TestMultiClient(t *testing.T) {

	zkAddr := "127.0.0.1"
	topic := "topic3"

	// zk client to reset
	zkClient := zookeeper.NewZKClient(zkAddr)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	brokerInstance := broker.NewBroker(zkAddr).WithLogLevel(testLogLevel)

	defer brokerInstance.Clean()
	defer SleepForBroker()

	brokerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := brokerInstance.Start(brokerCtx)
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}
	}()

	SleepForBroker()

	// Create topic rpc
	rpcClient := client.NewRPCClient(zkAddr)
	if err := rpcClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer rpcClient.Close()

	rpcCtx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := rpcClient.CreateTopic(rpcCtx, topic, "", 0, 0); err != nil {
		st := status.Convert(err)
		if st.Code() != codes.AlreadyExists {
			t.Error(err)
			return
		}
	}

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	// Start producer
	var expectedSentCount int
	var sentRecords [][]byte

	runProducer := func(ctx context.Context, fileName string) {
		count, sendingRecords := readFromFileLineBy(fileName)
		expectedSentCount += count
		wg.Add(1)
		go func() {
			defer wg.Done()

			producerClient := producer.NewProducer(zkAddr).WithLogLevel(testLogLevel)
			if err := producerClient.Connect(ctx, topic); err != nil {
				t.Error(err)
				return
			}

			for _, record := range sendingRecords {
				producerClient.Publish(ctx, record)
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

	runProducer(context.Background(), "data1.txt")
	runProducer(context.Background(), "data2.txt")
	runProducer(context.Background(), "data3.txt")

	time.Sleep(1 * time.Second)

	// Start consumer
	type ReceivedRecords [][]byte
	var totalReceivedRecords []ReceivedRecords

	runConsumer := func(ctx context.Context) {
		var receivedRecords ReceivedRecords

		wg.Add(1)
		go func() {
			defer wg.Done()

			consumerClient := consumer.NewConsumer(zkAddr).WithLogLevel(testLogLevel)
			if err := consumerClient.Connect(ctx, topic); err != nil {
				t.Error(err)
				return
			}

			receiveCount := 0
			subscribeCh, err := consumerClient.Subscribe(ctx, 0)
			if err != nil {
				t.Error(err)
				return
			}
			for response := range subscribeCh {
				if response.Error != nil {
					t.Error(response.Error)
				} else {

					receivedRecords = append(receivedRecords, response.Data)
					receiveCount++
					if expectedSentCount == receiveCount {
						fmt.Println("complete consumer. received :", receiveCount)
						break
					}
				}
			}

			totalReceivedRecords = append(totalReceivedRecords, receivedRecords)

			if err := consumerClient.Close(); err != nil {
				t.Error(err)
			}
		}()
	}

	runConsumer(context.Background())
	runConsumer(context.Background())

	wg.Wait()

	for _, receivedRecords := range totalReceivedRecords {
		if len(sentRecords) != len(receivedRecords) {
			t.Error("Length Mismatch - Expected records: ", len(sentRecords), ", Received records: ", len(receivedRecords))
		}

		for _, record := range receivedRecords {
			if !contains(sentRecords, record) {
				t.Error("Record is not exists: ", record)
			}
		}
	}
}
