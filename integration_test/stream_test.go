package integration_test

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/client"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
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

func TestStreamClient_Connect(t *testing.T) {
	brokerAddr := "127.0.0.1:1101"
	zkAddr := "127.0.0.1"
	topic := "test_topic1"

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
	defer brokerInstance.Stop()

	go func() {
		defer bwg.Done()
		brokerInstance.Start()
	}()

	Sleep(1)

	admin := client.NewAdmin(brokerAddr)
	if err := admin.Connect(); err != nil {
		t.Error(err)
		return
	}

	if err := admin.CreateTopic(topic, "meta", 1, 1); err != nil {
		t.Error(err)
		return
	}

	producer := client.NewProducer(brokerAddr, topic)
	defer producer.Close()
	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}

	consumer := client.NewConsumer(brokerAddr, topic)
	defer consumer.Close()
	if err := consumer.Connect(); err != nil {
		t.Error(err)
		return
	}
}

func TestPubSub(t *testing.T) {
	zkAddr := "127.0.0.1"

	expectedRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	topic := "topic1"
	actualRecords := make([][]byte, 0)

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
	defer brokerInstance.Stop()

	go func() {
		defer bwg.Done()
		brokerInstance.Start()
	}()

	Sleep(1)
	brokerAddr := "127.0.0.1:1101"
	// Create topic rpc
	admin := client.NewAdmin(brokerAddr)
	if err := admin.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer admin.Close()

	if err := admin.CreateTopic(topic, "", 1, 1); err != nil {
		t.Error(err)
		return
	}

	// Start producer
	producer := client.NewProducer(brokerAddr, topic).WithLogLevel(testLogLevel)
	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}
	publishCh := make(chan []byte)
	defer close(publishCh)

	partitionCh, pubErrCh, err := producer.AsyncPublish(publishCh)
	if err != nil {
		t.Error(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer producer.Close()

		published := 0
		for {
			select {
			case err := <-pubErrCh:
				if err != nil {
					t.Error(err)
					return
				}
			case partition := <-partitionCh:
				fmt.Println("publish succeed, offset :", partition.Offset)
				published++
				if published == len(expectedRecords) {
					return
				}
			}
		}
	}()

	consumer := client.NewConsumer(brokerAddr, topic).WithLogLevel(testLogLevel)
	if err := consumer.Connect(); err != nil {
		t.Error(err)
		return
	}

	receiveCh, subErrCh, err := consumer.Subscribe(0)
	if err != nil {
		t.Error(err)
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer consumer.Close()
		for {
			select {
			case received := <-receiveCh:
				actualRecords = append(actualRecords, received.Data)
				fmt.Println(len(actualRecords), len(expectedRecords))
				if len(actualRecords) == len(expectedRecords) {
					return
				}
			case err := <-subErrCh:
				t.Error(err)
				return
			}
		}
	}()

	for _, record := range expectedRecords {
		publishCh <- record
	}

	wg.Wait()
	for i, expectedRecord := range expectedRecords {
		if bytes.Compare(expectedRecord, actualRecords[i]) != 0 {
			t.Error("published records and subscribed records does not match")
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
	defer brokerInstance.Stop()

	go func() {
		defer bwg.Done()
		brokerInstance.Start()
	}()

	Sleep(1)

	brokerAddr := "127.0.0.1:1101"
	admin := client.NewAdmin(brokerAddr)
	if err := admin.Connect(); err != nil {
		t.Error(err)
		return
	}

	if err := admin.CreateTopic(topic, "meta", 1, 1); err != nil {
		t.Error(err)
		return
	}

	runProducer := func(fileName string) [][]byte {
		records := getRecordsFromFile(fileName)
		producer := client.NewProducer(brokerAddr, topic)
		if err := producer.Connect(); err != nil {
			t.Error(err)
			return nil
		}

		publishCh := make(chan []byte)

		partitionCh, pubErrCh, err := producer.AsyncPublish(publishCh)
		if err != nil {
			t.Error(err)
			return nil
		}

		go func() {
			defer close(publishCh)
			defer producer.Close()
			published := 0
			for {
				select {
				case err := <-pubErrCh:
					if err != nil {
						t.Error(err)
						return
					}
				case <-partitionCh:
					published++
					if published == len(records) {
						return
					}
				}
			}
		}()

		go func() {
			for _, record := range records {
				publishCh <- record
			}
		}()

		return records
	}

	// Start producer
	var totalPublishedRecords [][]byte

	totalPublishedRecords = append(totalPublishedRecords, runProducer("data1.txt")...)
	totalPublishedRecords = append(totalPublishedRecords, runProducer("data2.txt")...)
	totalPublishedRecords = append(totalPublishedRecords, runProducer("data3.txt")...)

	// Start consumer
	type SubscribedRecords [][]byte
	var totalSubscribedRecords []SubscribedRecords

	runConsumer := func() SubscribedRecords {
		var subscribedRecords SubscribedRecords
		consumer := client.NewConsumer(brokerAddr, topic).WithLogLevel(testLogLevel)
		if err := consumer.Connect(); err != nil {
			t.Error(err)
			return nil
		}

		defer consumer.Close()

		receiveCh, subErrCh, err := consumer.Subscribe(0)
		if err != nil {
			t.Error(err)
			return nil
		}

		for {
			select {
			case received := <-receiveCh:
				subscribedRecords = append(subscribedRecords, received.Data)
				if len(subscribedRecords) == len(totalPublishedRecords) {
					return subscribedRecords
				}
			case err := <-subErrCh:
				t.Error(err)
				return subscribedRecords
			}
		}
	}

	totalSubscribedRecords = append(totalSubscribedRecords, runConsumer())
	totalSubscribedRecords = append(totalSubscribedRecords, runConsumer())

	for _, subscribedRecords := range totalSubscribedRecords {
		if len(totalPublishedRecords) != len(subscribedRecords) {
			t.Error("Length Mismatch - Expected records: ", len(totalPublishedRecords), ", Received records: ", len(subscribedRecords))
		}

		for _, record := range subscribedRecords {
			if !contains(totalPublishedRecords, record) {
				t.Error("Record is not exists: ", record)
			}
		}
	}
}
