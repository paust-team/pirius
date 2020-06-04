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
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var testLogLevel = logger.Debug

func Sleep(sec int) {
	time.Sleep(time.Duration(sec) * time.Second)
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
	bwg := sync.WaitGroup{}
	bwg.Add(1)
	defer brokerInstance.Clean()
	defer bwg.Wait()

	go func() {
		defer bwg.Done()
		defer brokerInstance.Stop()
		brokerInstance.Start()
	}()

	Sleep(1)

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

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())

	defer cancel1()
	defer cancel2()

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
	bwg := sync.WaitGroup{}
	bwg.Add(1)
	defer brokerInstance.Clean()
	defer bwg.Wait()

	go func() {
		defer bwg.Done()
		defer brokerInstance.Stop()
		brokerInstance.Start()
	}()

	Sleep(1)

	// Create topic rpc
	rpcClient := client.NewRPCClient(zkAddr)
	if err := rpcClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer rpcClient.Close()

	rpcCtx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := rpcClient.CreateTopic(rpcCtx, topic, "", 1, 1); err != nil {
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
	publishCh := make(chan []byte)
	defer close(publishCh)
	errChP := producerClient.Publish(ctx1, publishCh)

	go func() {
		select {
		case err := <-errChP:
			if err != nil {
				t.Error(err)
			}
			return
		case <-ctx1.Done():
			return
		}
	}()

	for _, record := range testRecordMap[topic] {
		publishCh <- record
	}

	Sleep(1)

	// Start consumer
	consumerClient := consumer.NewConsumer(zkAddr).WithLogLevel(testLogLevel)
	if err := consumerClient.Connect(ctx2, topic); err != nil {
		t.Error(err)
		return
	}

	subscribeCh, errChC := consumerClient.Subscribe(ctx2, 0)

	go func() {
		select {
		case err := <-errChC:
			if err != nil {
				t.Error(err)
			}
			return
		case <-ctx2.Done():
			return
		}
	}()

subscribeUntil:
	for {
		select {
		case response:= <- subscribeCh:
			receivedRecordMap[topic] = append(receivedRecordMap[topic], response.Data)

			// break on reach end
			if len(testRecordMap[topic]) == len(receivedRecordMap[topic]) {
				break subscribeUntil
			}
		case <- time.After(time.Second * 10):
			t.Error("subscribe timeout")
			os.Exit(1)
		}
	}

	if err := producerClient.Close(); err != nil {
		t.Error(err)
	}

	if err := consumerClient.Close(); err != nil {
		t.Error(err)
	}

	// test
	for i, record := range testRecordMap[topic] {
		if bytes.Compare(receivedRecordMap[topic][i], record) != 0 {
			t.Error("Record is not same")
		}
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
	bwg := sync.WaitGroup{}
	bwg.Add(1)
	defer brokerInstance.Clean()
	defer bwg.Wait()

	go func() {
		defer bwg.Done()
		defer brokerInstance.Stop()
		brokerInstance.Start()
	}()

	Sleep(1)

	// Create topic rpc
	rpcClient := client.NewRPCClient(zkAddr)
	if err := rpcClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer rpcClient.Close()

	rpcCtx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := rpcClient.CreateTopic(rpcCtx, topic, "", 1, 1); err != nil {
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
			publishCh := make(chan []byte)
			defer close(publishCh)
			errCh := producerClient.Publish(ctx, publishCh)

			go func() {
				select {
				case err := <-errCh:
					if err != nil {
						t.Error(err)
					}
					return
				case <-ctx.Done():
					return
				}
			}()

			for _, record := range sendingRecords {
				publishCh <- record
			}

			mu.Lock()
			sentRecords = append(sentRecords, sendingRecords...)
			mu.Unlock()

			Sleep(2)

			if err := producerClient.Close(); err != nil {
				t.Error(err)
			}
			fmt.Println("publish done with file :", fileName, "sent total:", len(sendingRecords))
		}()
	}

	ctx1, cancelP1 := context.WithCancel(context.Background())
	ctx2, cancelP2 := context.WithCancel(context.Background())
	ctx3, cancelP3 := context.WithCancel(context.Background())
	defer cancelP1()
	defer cancelP2()
	defer cancelP3()

	runProducer(ctx1, "data1.txt")
	runProducer(ctx2, "data2.txt")
	runProducer(ctx3, "data3.txt")

	Sleep(1)

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
			subscribeCh, errCh := consumerClient.Subscribe(ctx, 0)

			go func() {
				select {
				case err := <-errCh:
					if err != nil {
						t.Error(err)
					}
					return
				case <-ctx.Done():
					return
				}
			}()

		subscribeUntil:
			for {
				select {
				case response:= <- subscribeCh:
					receivedRecords = append(receivedRecords, response.Data)
					receiveCount++
					if expectedSentCount == receiveCount {
						fmt.Println("complete consumer. received :", receiveCount)
						break subscribeUntil
					}
				case <- time.After(time.Second * 3):
					t.Error("subscribe timeout")
					return
				}
			}

			totalReceivedRecords = append(totalReceivedRecords, receivedRecords)

			if err := consumerClient.Close(); err != nil {
				t.Error(err)
			}
		}()
	}

	ctx4, cancelC1 := context.WithCancel(context.Background())
	ctx5, cancelC2 := context.WithCancel(context.Background())
	defer cancelC1()
	defer cancelC2()

	runConsumer(ctx4)
	runConsumer(ctx5)

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
