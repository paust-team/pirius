package integration_test

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/client"
	config2 "github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var testLogLevel = logger.Info
var brokerPort uint = 1101
var brokerAddrs = []string{"127.0.0.1:1101"}
var zkAddrs = []string{"127.0.0.1:2181"}
var zkTimeoutMS uint = 3000
var zkFlushIntervalMS uint = 2000

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

func setupTopicAndFragments(topic string, fragmentCount int, t *testing.T) map[uint32]uint64 {
	fragmentOffsets := make(map[uint32]uint64)
	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(testLogLevel)
	adminConfig.SetServerAddresses(brokerAddrs)
	admin := client.NewAdmin(adminConfig)
	defer admin.Close()

	if err := admin.Connect(); err != nil {
		panic(err)
	}

	if err := admin.CreateTopic(topic, "test"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < fragmentCount; i++ {
		fragment, err := admin.CreateFragment(topic)
		if err != nil {
			t.Fatal(err)
		}
		fragmentOffsets[fragment.Id] = 1 // set start offset with 1
	}
	return fragmentOffsets
}

func TestStreamClient_Connect(t *testing.T) {
	// zk client to reset
	zkClient := zookeeper.NewZKQClient(zkAddrs, zkTimeoutMS, zkFlushIntervalMS)
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
	brokerConfig.SetZKQuorum(zkAddrs)
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

	// setup topic and fragments
	topic := "test_topic1"
	fragmentCount := 1
	fragmentOffsets := setupTopicAndFragments(topic, fragmentCount, t)

	// test connect
	producerConfig := config2.NewProducerConfig()
	producerConfig.SetLogLevel(testLogLevel)
	producerConfig.SetServerAddresses(zkAddrs)
	producer := client.NewProducer(producerConfig, topic)
	defer producer.Close()
	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}

	consumerConfig := config2.NewConsumerConfig()
	consumerConfig.SetLogLevel(testLogLevel)
	consumerConfig.SetServerAddresses(zkAddrs)
	consumer := client.NewConsumer(consumerConfig, topic, fragmentOffsets)
	defer consumer.Close()
	if err := consumer.Connect(); err != nil {
		t.Error(err)
		return
	}
}

func TestPubSub(t *testing.T) {

	// zk client to reset
	zkClient := zookeeper.NewZKQClient(zkAddrs, zkTimeoutMS, zkFlushIntervalMS)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	// Start broker
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKQuorum(zkAddrs)
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

	// setup topic and fragments
	expectedRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	nodeId := common.GenerateNodeId()
	actualRecords := make([][]byte, 0)
	topic := "topic1"
	fragmentCount := 1
	fragmentOffsets := setupTopicAndFragments(topic, fragmentCount, t)

	// Start producer
	producerConfig := config2.NewProducerConfig()
	producerConfig.SetLogLevel(testLogLevel)
	producerConfig.SetServerAddresses(zkAddrs)
	producer := client.NewProducer(producerConfig, topic)
	if err := producer.Connect(); err != nil {
		t.Error(err)
		return
	}
	publishCh := make(chan *client.PublishData)
	defer close(publishCh)

	fragmentCh, pubErrCh, err := producer.AsyncPublish(publishCh)
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
			case fragment := <-fragmentCh:
				fmt.Printf("publish succeed. fragmentId=%d offset=%d\n", fragment.FragmentId, fragment.LastOffset)
				published++
				if published == len(expectedRecords) {
					return
				}
			}
		}
	}()

	consumerConfig := config2.NewConsumerConfig()
	consumerConfig.SetLogLevel(testLogLevel)
	consumerConfig.SetServerAddresses(zkAddrs)
	consumer := client.NewConsumer(consumerConfig, topic, fragmentOffsets)
	if err := consumer.Connect(); err != nil {
		t.Error(err)
		return
	}

	receiveCh, subErrCh, err := consumer.Subscribe(1, 0)
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
				actualRecords = append(actualRecords, received.Items[0].Data)
				fmt.Printf("received fetch result. fragmentId = %d, seq = %d, node id = %s\n",
					received.Items[0].FragmentId, received.Items[0].SeqNum, received.Items[0].NodeId)
				if len(actualRecords) == len(expectedRecords) {
					return
				}
			case err := <-subErrCh:
				t.Error(err)
				return
			}
		}
	}()

	for index, record := range expectedRecords {
		publishCh <- &client.PublishData{
			Data:   record,
			NodeId: nodeId,
			SeqNum: uint64(index),
		}
	}

	wg.Wait()
	for _, record := range expectedRecords {
		if !contains(actualRecords, record) {
			t.Error("Record is not exists: ", record)
		}
	}

	time.Sleep(time.Duration(3) * time.Second) // sleep 3 seconds to wait until last offset to be flushed to zk
	for fragmentId := range fragmentOffsets {
		fragmentValue, err := zkClient.GetTopicFragmentData(topic, fragmentId)
		if err != nil {
			t.Fatal(err)
		}

		if uint64(len(expectedRecords)) != fragmentValue.LastOffset() {
			t.Errorf("expected last offset (%d) is not matched with (%d)", len(expectedRecords), fragmentValue.LastOffset())
		}
	}
}

func TestMultiClient(t *testing.T) {

	//zk client to reset
	zkClient := zookeeper.NewZKQClient(zkAddrs, zkTimeoutMS, zkFlushIntervalMS)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKQuorum(zkAddrs)
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

	// setup topic and fragments
	topic := "topic3"
	fragmentCount := 1
	fragmentOffsets := setupTopicAndFragments(topic, fragmentCount, t)

	// test multi client
	runProducer := func(fileName string) [][]byte {
		nodeId := common.GenerateNodeId()
		records := getRecordsFromFile(fileName)

		producerConfig := config2.NewProducerConfig()
		producerConfig.SetLogLevel(testLogLevel)
		producerConfig.SetServerAddresses(zkAddrs)
		producerConfig.SetBrokerTimeout(100000)
		producer := client.NewProducer(producerConfig, topic)
		if err := producer.Connect(); err != nil {
			t.Error(err)
			return nil
		}

		publishCh := make(chan *client.PublishData)

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
						fmt.Printf("done producer with file %s\n", fileName)
						return
					}
				}
			}
		}()

		go func() {
			for index, record := range records {
				publishCh <- &client.PublishData{
					Data:   record,
					NodeId: nodeId,
					SeqNum: uint64(index),
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
		consumerConfig.SetServerAddresses(zkAddrs)
		consumerConfig.SetBrokerTimeout(30000)
		consumer := client.NewConsumer(consumerConfig, topic, fragmentOffsets)
		if err := consumer.Connect(); err != nil {
			t.Error(err)
			wg.Done()
			return
		}

		receiveCh, subErrCh, err := consumer.Subscribe(1, 0)
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
				case received := <-receiveCh:
					subscribedRecords = append(subscribedRecords, received.Items[0].Data)
					if len(subscribedRecords) == len(totalPublishedRecords) {
						fmt.Printf("done %s\n", name)
						mu.Lock()
						totalSubscribedRecords = append(totalSubscribedRecords, subscribedRecords)
						mu.Unlock()
						return
					}
				case err := <-subErrCh:
					t.Error(err)
					return
				}
			}
		}()
	}

	consumerCount := 1
	wg.Add(consumerCount)

	for i := 0; i < consumerCount; i++ {
		runConsumer(fmt.Sprintf("consumer%d", i))
	}

	wg.Wait()

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

func TestBatchClient(t *testing.T) {
	//zk client to reset
	zkClient := zookeeper.NewZKQClient(zkAddrs, zkTimeoutMS, zkFlushIntervalMS)
	if err := zkClient.Connect(); err != nil {
		t.Error(err)
		return
	}
	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	brokerConfig := config.NewBrokerConfig()
	brokerConfig.SetLogLevel(testLogLevel)
	brokerConfig.SetZKQuorum(zkAddrs)
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

	// setup topic and fragments
	var maxBatchSize uint32 = 32
	var flushInterval uint32 = 200
	topic := "topic4"
	fragmentCount := 1
	fragmentOffsets := setupTopicAndFragments(topic, fragmentCount, t)

	// test batch client
	runProducer := func(fileName string) [][]byte {
		nodeId := common.GenerateNodeId()
		records := getRecordsFromFile(fileName)

		producerConfig := config2.NewProducerConfig()
		producerConfig.SetLogLevel(testLogLevel)
		producerConfig.SetServerAddresses(zkAddrs)
		producerConfig.SetBrokerTimeout(100000)
		producer := client.NewProducer(producerConfig, topic)
		if err := producer.Connect(); err != nil {
			t.Error(err)
			return nil
		}

		publishCh := make(chan *client.PublishData)

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
						fmt.Printf("done producer with file %s\n", fileName)
						return
					}
				}
			}
		}()

		go func() {
			for index, record := range records {
				publishCh <- &client.PublishData{
					Data:   record,
					NodeId: nodeId,
					SeqNum: uint64(index),
				}
			}
		}()

		return records
	}

	// Start producer
	var totalPublishedRecords [][]byte
	totalPublishedRecords = append(totalPublishedRecords, runProducer("data1.txt")...)

	// Start consumer
	type SubscribedRecords [][]byte
	var totalSubscribedRecords []SubscribedRecords
	var mu sync.Mutex
	var wg sync.WaitGroup

	runConsumer := func(name string) {
		var subscribedRecords SubscribedRecords

		consumerConfig := config2.NewConsumerConfig()
		consumerConfig.SetLogLevel(testLogLevel)
		consumerConfig.SetServerAddresses(zkAddrs)
		consumerConfig.SetBrokerTimeout(30000)
		consumer := client.NewConsumer(consumerConfig, topic, fragmentOffsets)
		if err := consumer.Connect(); err != nil {
			t.Error(err)
			wg.Done()
			return
		}

		receiveCh, subErrCh, err := consumer.Subscribe(maxBatchSize, flushInterval)
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
				case received := <-receiveCh:
					for _, data := range received.Items {
						subscribedRecords = append(subscribedRecords, data.Data)
					}

					if len(subscribedRecords) == len(totalPublishedRecords) {
						fmt.Printf("done %s\n", name)
						mu.Lock()
						totalSubscribedRecords = append(totalSubscribedRecords, subscribedRecords)
						mu.Unlock()
						return
					}
				case err := <-subErrCh:
					t.Error(err)
					return
				}
			}
		}()
	}

	wg.Add(2)
	runConsumer(fmt.Sprintf("consumer%d", 1))
	runConsumer(fmt.Sprintf("consumer%d", 2))
	wg.Wait()

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
