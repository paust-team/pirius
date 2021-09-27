package main

import (
	"encoding/json"
	"github.com/paust-team/shapleq/benchmark/kafka"
	"github.com/paust-team/shapleq/benchmark/shapleQ"
	"github.com/paust-team/shapleq/common"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type BenchClient interface {
	CreateTopic(string)
	DeleteTopic(string)
	RunProducer(string, string, string, int) (int64, int64)
	RunConsumer(string, string, int) (int64, int64)
}

type TimePair struct {
	Start int64
	End   int64
}
type Result struct {
	NumClient    int
	MinTime      int64
	MaxTime      int64
	AvgTime      float64
	ElapsedTimes []int64
}

func makeTotalResult(numClient int, producerTimes, consumerTimes []TimePair) *Result {
	var sum int64 = 0
	var min int64 = 0
	var max int64 = 0

	var minStartTime int64 = 0

	var elapsedTimes []int64
	for i, t := range producerTimes {
		if i == 0 || t.Start < min {
			minStartTime = t.Start
		}
	}

	for i, t := range consumerTimes {
		endTime := t.End
		endTime -= minStartTime
		elapsedTimes = append(elapsedTimes, endTime)
		sum += endTime
		if i == 0 || endTime < min {
			min = endTime
		}
		if i == 0 || endTime > max {
			max = endTime
		}
	}
	avg := (float64(sum)) / (float64(len(consumerTimes)))

	return &Result{NumClient: numClient, MinTime: min, MaxTime: max, AvgTime: avg, ElapsedTimes: elapsedTimes}
}

func makeResult(numClient int, resultTimes []TimePair) *Result {
	var sum int64 = 0
	var min int64 = 0
	var max int64 = 0

	var elapsedTimes []int64

	for i, t := range resultTimes {
		endTime := t.End - t.Start
		elapsedTimes = append(elapsedTimes, endTime)
		sum += endTime
		if i == 0 || endTime < min {
			min = endTime
		}
		if i == 0 || endTime > max {
			max = endTime
		}
	}
	avg := (float64(sum)) / (float64(len(resultTimes)))

	return &Result{NumClient: numClient, MinTime: min, MaxTime: max, AvgTime: avg, ElapsedTimes: elapsedTimes}
}

func saveResult(result *Result, path string) {

	jsonResult, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	if _, err := file.Write(jsonResult); err != nil {
		panic(err)
	}
	if _, err := file.WriteString("\n"); err != nil {
		panic(err)
	}
}

func runBenchClient(client BenchClient, topic string, numProducer int, numConsumer int,
	filePath string, numData int) ([]TimePair, []TimePair) {

	client.CreateTopic(topic)
	defer client.DeleteTopic(topic)

	var producerResults []TimePair
	var consumerResults []TimePair

	pMu := sync.Mutex{}
	cMu := sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(numProducer)
	wg.Add(numConsumer)

	client.RunProducer(common.GenerateNodeId(), topic, filePath, 1) // pre send one data for shapleQ consumer

	for i := 0; i < numConsumer; i++ {
		go func(id int) {
			defer wg.Done()
			start, end := client.RunConsumer(common.GenerateNodeId(), topic, numData)
			cMu.Lock()
			consumerResults = append(consumerResults, TimePair{start, end})
			cMu.Unlock()
		}(i)
	}

	time.Sleep(1 * time.Second) // wait for all consumer goroutine to start

	for i := 0; i < numProducer; i++ {
		go func(id int) {
			defer wg.Done()
			start, end := client.RunProducer(common.GenerateNodeId(), topic, filePath, numData-1)
			pMu.Lock()
			producerResults = append(producerResults, TimePair{start, end})
			pMu.Unlock()
		}(i)
	}

	wg.Wait()

	return producerResults, consumerResults
}

func runBenchClientStored(client BenchClient, topic string, numProducer int, numConsumer int,
	filePath string, numData int) ([]TimePair, []TimePair) {

	client.CreateTopic(topic)
	defer client.DeleteTopic(topic)

	var producerResults []TimePair
	var consumerResults []TimePair

	pMu := sync.Mutex{}
	cMu := sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(numProducer)

	for i := 0; i < numProducer; i++ {
		go func(id int) {
			defer wg.Done()
			start, end := client.RunProducer(common.GenerateNodeId(), topic, filePath, numData)
			pMu.Lock()
			producerResults = append(producerResults, TimePair{start, end})
			pMu.Unlock()
		}(i)
	}
	wg.Wait()

	wg.Add(numConsumer)
	for i := 0; i < numConsumer; i++ {
		go func(id int) {
			defer wg.Done()
			start, end := client.RunConsumer(common.GenerateNodeId(), topic, numData)
			cMu.Lock()
			consumerResults = append(consumerResults, TimePair{start, end})
			cMu.Unlock()
		}(i)
	}

	wg.Wait()

	return producerResults, consumerResults
}

func test(topic string, numProducer int, numConsumer int, numData int, target string, testLiveData bool) {

	kafkaHost := "3.35.5.233"
	brokerHost := "3.35.5.233"
	var brokerPort uint = 1101

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	testFilePath := path + "/test-dataset.tsv"
	timeout := 30000 * time.Millisecond

	if testLiveData {
		if target == "kf" {
			// Test Kafka
			kafkaBenchClient := kafka.NewBenchKafkaClient(kafkaHost, timeout)
			runBenchClient(kafkaBenchClient, "preheat", 1, 1, testFilePath, 10000) // preheat
			kfProducerResults, kfConsumerResults := runBenchClient(kafkaBenchClient, topic, numProducer, numConsumer, testFilePath, numData)
			saveResult(makeTotalResult(numConsumer, kfProducerResults, kfConsumerResults), path+"/kf-result.txt")
		} else {
			// Test ShapleQ
			shapleQBenchClient := shapleQ.NewBenchShapleQClient(brokerHost, brokerPort, timeout)
			runBenchClient(shapleQBenchClient, "preheat", 1, 1, testFilePath, 10000) // preheat
			sqProducerResults, sqConsumerResults := runBenchClient(shapleQBenchClient, topic, numProducer, numConsumer, testFilePath, numData)
			saveResult(makeTotalResult(numConsumer, sqProducerResults, sqConsumerResults), path+"/sq-result.txt")
		}
	} else {
		if target == "kf" {
			// Test Kafka
			kafkaBenchClient := kafka.NewBenchKafkaClient(kafkaHost, timeout)
			runBenchClientStored(kafkaBenchClient, "preheat", 1, 1, testFilePath, 10000) // preheat
			_, kfConsumerResults := runBenchClientStored(kafkaBenchClient, topic, numProducer, numConsumer, testFilePath, numData)
			saveResult(makeResult(numConsumer, kfConsumerResults), path+"/kf-result-stored.txt")
		} else {
			// Test ShapleQ
			shapleQBenchClient := shapleQ.NewBenchShapleQClient(brokerHost, brokerPort, timeout)
			runBenchClientStored(shapleQBenchClient, "preheat", 1, 1, testFilePath, 10000) // preheat
			_, sqConsumerResults := runBenchClientStored(shapleQBenchClient, topic, numProducer, numConsumer, testFilePath, numData)
			saveResult(makeResult(numConsumer, sqConsumerResults), path+"/sq-result-stored.txt")
		}
	}
}

func main() {
	if len(os.Args) != 6 {
		log.Fatal("Usage: ./benchmark [topic] [num-producer] [num-consumer] [num-data] [sq|kf]")
	}

	topic := os.Args[1]
	numProducer, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	numConsumer, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}
	numData, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}
	testTarget := os.Args[5]

	test(topic, numProducer, numConsumer, numData, testTarget, true)
}
