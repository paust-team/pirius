package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/client"
	config2 "github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
	"log"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

var testLogLevel = logger.Error
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

	defer admin.Close()

	producerConfig := config2.NewProducerConfig()
	producerConfig.SetLogLevel(testLogLevel)
	producerConfig.SetBrokerHost(brokerHost)
	producerConfig.SetBrokerPort(brokerPort)
	producer := client.NewProducer(producerConfig, topic)

	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}

	defer producer.Close()

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
		{'2', '2', '3', '4', '5', '6'},
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
	brokerConfig.SetTimeout(10000)
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

	fmt.Println("after broker start", runtime.NumGoroutine())

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

	if err := admin.CreateTopic(topic, "", 1, 1); err != nil {
		t.Error(err)
		return
	}

	admin.Close()

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
				}
				return
			case partition, ok := <-partitionCh:
				if ok {
					fmt.Println("publish succeed, offset :", partition.Offset)
					published++
					if published == len(expectedRecords) {
						return
					}
				} else {
					return
				}
			}
		}
	}()

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer consumer.Close()
		for {
			select {
			case received, ok := <-receiveCh:
				if ok {
					actualRecords = append(actualRecords, received.Data)
					fmt.Println("subscribe offset : ", received.Offset)
					if len(actualRecords) == len(expectedRecords) {
						return
					}
				} else {
					return
				}

			case err, ok := <-subErrCh:
				if ok {
					t.Error(err)
				}
				return
			}
		}
	}()

	time.Sleep(1 * time.Second)
	fmt.Println("before publish start", runtime.NumGoroutine())

	for _, record := range expectedRecords {
		publishCh <- record
	}

	wg.Wait()
	time.Sleep(2 * time.Second)

	fmt.Println("after consumer, producer is closed", runtime.NumGoroutine())

	if len(expectedRecords) != len(actualRecords) {
		t.Error("Length not matched")
	}
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
	brokerConfig.SetTimeout(100000)
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

	admin.Close()
	Sleep(1)
	fmt.Println("before pub/sub", runtime.NumGoroutine())

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

		publishCh := make(chan []byte)

		partitionCh, pubErrCh, err := producer.AsyncPublish(publishCh)
		if err != nil {
			t.Error(err)
			return nil
		}

		receiveCtx, cancel := context.WithCancel(context.Background())
		go func() {
			published := 0
			defer producer.Close()
			defer cancel()

			for {
				select {
				case err := <-pubErrCh:
					if err != nil {
						t.Error(err)
					}
					return
				case _, ok := <-partitionCh:
					if ok {
						published++
						if published == len(records) {
							fmt.Printf("done producer with file %s\n", fileName)
							return
						}
					}
				}
			}
		}()

		go func() {
			defer close(publishCh)

			for _, record := range records {
				select {
				case <-receiveCtx.Done():
					return
				default:
					publishCh <- record
				}
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
	var mu sync.Mutex
	var wg sync.WaitGroup

	runConsumer := func(name string) {
		var subscribedRecords SubscribedRecords

		consumerConfig := config2.NewConsumerConfig()
		consumerConfig.SetLogLevel(testLogLevel)
		consumerConfig.SetBrokerHost(brokerHost)
		consumerConfig.SetBrokerPort(brokerPort)
		consumer := client.NewConsumer(consumerConfig, topic)
		if err := consumer.Connect(); err != nil {
			t.Error(err)
			wg.Done()
			return
		}

		receiveCh, subErrCh, err := consumer.Subscribe(0)
		if err != nil {
			t.Error(err)
			wg.Done()
			return
		}

		go func() {
			defer wg.Done()
			defer consumer.Close()

			for {
				select {
				case received, ok := <-receiveCh:
					if ok {
						subscribedRecords = append(subscribedRecords, received.Data)
						if len(subscribedRecords) == len(totalPublishedRecords) {
							fmt.Printf("done %s\n", name)
							mu.Lock()
							totalSubscribedRecords = append(totalSubscribedRecords, subscribedRecords)
							mu.Unlock()
							return
						}
					} else {
						return
					}

				case err, ok := <-subErrCh:
					if ok {
						t.Error(err)
					}
					return
				}
			}
		}()
	}

	consumerCount := 32
	wg.Add(consumerCount)

	for i := 0; i < consumerCount; i++ {
		runConsumer(fmt.Sprintf("consumer%d", i))
	}

	wg.Wait()

	Sleep(1)
	fmt.Println("after wait", runtime.NumGoroutine())

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
