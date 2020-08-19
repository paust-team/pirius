package integration_test

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/client"
	config2 "github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var testLogLevel = logger.Debug
var brokerPort uint = 1101
var brokerHost = "127.0.0.1"
var zkAddr = "127.0.0.1"

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
	topic := "test_topic1"

	// zk client to reset
	zkClient := zookeeper.NewZKClient(zkAddr, 3000)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	// Start broker
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetPort(brokerPort)
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKHost(zkAddr)
	brokerInstance := broker.NewBroker(brokerConfig)
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

	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(testLogLevel)
	adminConfig.SetBrokerHost(brokerHost)
	adminConfig.SetBrokerPort(brokerPort)
	admin := client.NewAdmin(adminConfig)
	if err := admin.Connect(); err != nil {
		t.Error(err)
		return
	}

	if err := admin.CreateTopic(topic, "meta", 1, 1); err != nil {
		t.Error(err)
		return
	}

	producerConfig := config2.NewProducerConfig()
	producerConfig.SetLogLevel(testLogLevel)
	producerConfig.SetBrokerHost(brokerHost)
	producerConfig.SetBrokerPort(brokerPort)
	producer := client.NewProducer(producerConfig, topic)
	defer producer.Close()
	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}

	consumerConfig := config2.NewConsumerConfig()
	consumerConfig.SetLogLevel(testLogLevel)
	consumerConfig.SetBrokerHost(brokerHost)
	consumerConfig.SetBrokerPort(brokerPort)
	consumer := client.NewConsumer(consumerConfig, topic)
	defer consumer.Close()
	if err := consumer.Connect(); err != nil {
		t.Error(err)
		return
	}
}

func TestPubSub(t *testing.T) {

	expectedRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	topic := "topic1"
	actualRecords := make([][]byte, 0)

	// zk client to reset
	zkClient := zookeeper.NewZKClient(zkAddr, 3000)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	// Start broker
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKHost(zkAddr)
	brokerInstance := broker.NewBroker(brokerConfig)
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

	// Create topic rpc
	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(testLogLevel)
	adminConfig.SetBrokerHost(brokerHost)
	adminConfig.SetBrokerPort(brokerPort)
	admin := client.NewAdmin(adminConfig)
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
	producerConfig := config2.NewProducerConfig()
	producerConfig.SetLogLevel(testLogLevel)
	producerConfig.SetBrokerHost(brokerHost)
	producerConfig.SetBrokerPort(brokerPort)
	producer := client.NewProducer(producerConfig, topic)
	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}

	if _, err := producer.Publish(expectedRecords[0]); err != nil {
		t.Error(err)
	}

	consumerConfig := config2.NewConsumerConfig()
	consumerConfig.SetLogLevel(testLogLevel)
	consumerConfig.SetBrokerHost(brokerHost)
	consumerConfig.SetBrokerPort(brokerPort)
	consumer := client.NewConsumer(consumerConfig, topic)
	if err := consumer.Connect(); err != nil {
		t.Error(err)
		return
	}

	receiveCh, subErrCh, err := consumer.Subscribe(0)
	if err != nil {
		t.Error(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer consumer.Close()
		for {
			select {
			case received := <-receiveCh:
				actualRecords = append(actualRecords, received.Data)
				fmt.Println(len(actualRecords), len(expectedRecords), string(received.Data))
				if len(actualRecords) == len(expectedRecords) {
					return
				}
			case err := <-subErrCh:
				t.Fatal(err)
				return
			}
		}
	}()

	for _, record := range expectedRecords[1:] {
		if _, err := producer.Publish(record); err != nil {
			t.Error(err)
		}
	}

	wg.Wait()
	for i, expectedRecord := range expectedRecords {
		if bytes.Compare(expectedRecord, actualRecords[i]) != 0 {
			t.Error("published records and subscribed records does not match")
		}
	}
}

func TestMultiClient(t *testing.T) {
	topic := "topic3"

	// zk client to reset
	zkClient := zookeeper.NewZKClient(zkAddr, 3000)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKHost(zkAddr)
	brokerInstance := broker.NewBroker(brokerConfig)
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

	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(testLogLevel)
	adminConfig.SetBrokerHost(brokerHost)
	adminConfig.SetBrokerPort(brokerPort)
	admin := client.NewAdmin(adminConfig)
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

		producerConfig := config2.NewProducerConfig()
		producerConfig.SetLogLevel(testLogLevel)
		producerConfig.SetBrokerHost(brokerHost)
		producerConfig.SetBrokerPort(brokerPort)
		producer := client.NewProducer(producerConfig, topic)
		if err := producer.Connect(); err != nil {
			t.Error(err)
			return nil
		}

		if _, err := producer.Publish(records[0]); err != nil {
			t.Error(err)
		}

		go func() {
			for _, record := range records[1:] {
				if _, err := producer.Publish(record); err != nil {
					t.Error(err)
				}
			}
		}()

		return records
	}

	// Start producer
	var totalPublishedRecords [][]byte

	totalPublishedRecords = append(totalPublishedRecords, runProducer("data1.txt")...)
	/* TODO::
		There is a bug on multi-producer test due to tailing iterator.
		Multi-producer should be tested after implementing the backpressure functionality later.
	//totalPublishedRecords = append(totalPublishedRecords, runProducer("data2.txt")...)
	//totalPublishedRecords = append(totalPublishedRecords, runProducer("data3.txt")...)
	*/

	// Start consumer
	type SubscribedRecords [][]byte
	var totalSubscribedRecords []SubscribedRecords

	runConsumer := func() SubscribedRecords {
		var subscribedRecords SubscribedRecords

		consumerConfig := config2.NewConsumerConfig()
		consumerConfig.SetLogLevel(testLogLevel)
		consumerConfig.SetBrokerHost(brokerHost)
		consumerConfig.SetBrokerPort(brokerPort)
		consumerConfig.SetTimeout(100000)
		consumer := client.NewConsumer(consumerConfig, topic)
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

		var prevOffset uint64 = 0
		for {
			select {
			case received := <-receiveCh:
				subscribedRecords = append(subscribedRecords, received.Data)
				if len(subscribedRecords) == len(totalPublishedRecords) {
					return subscribedRecords
				}
				if prevOffset+1 != received.Offset {
					if received.Offset != 0 {
						t.Error("missing offset ", received.Offset, prevOffset)
					}
				}

				prevOffset = received.Offset

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
