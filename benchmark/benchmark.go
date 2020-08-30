package main

import (
	"encoding/json"
	"github.com/paust-team/shapleq/benchmark/kafka"
	"github.com/paust-team/shapleq/benchmark/shapleQ"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type BenchClient interface {
	CreateTopic(string)
	DeleteTopic(string)
	RunProducer(int, string, string, int) int64
	RunConsumer(int, string, int) int64
}

type Result struct {
	NumClient    int
	MinTime      int64
	MaxTime      int64
	AvgTime      float64
	ElapsedTimes []int64
}

func makeResult(numClient int, elapsedTimes []int64) *Result {
	var sum int64 = 0
	var min int64 = 0
	var max int64 = 0

	for i, et := range elapsedTimes {
		sum += et
		if i == 0 || et < min {
			min = et
		}
		if i == 0 || et > max {
			max = et
		}
	}
	avg := (float64(sum)) / (float64(len(elapsedTimes)))

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
	filePath string, numData int) ([]int64, []int64) {

	client.CreateTopic(topic)
	defer client.DeleteTopic(topic)

	var producerResults []int64
	var consumerResults []int64

	pMu := sync.Mutex{}
	cMu := sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(numProducer)
	wg.Add(numConsumer)

	client.RunProducer(0, topic, filePath, 1) // pre send one data for shapleQ consumer

	for i := 0; i < numConsumer; i++ {
		go func(id int) {
			defer wg.Done()
			result := client.RunConsumer(id, topic, numData)
			cMu.Lock()
			consumerResults = append(consumerResults, result)
			cMu.Unlock()
		}(i)
	}

	time.Sleep(1 * time.Second) // wait for all consumer goroutine to start

	for i := 0; i < numProducer; i++ {
		go func(id int) {
			defer wg.Done()
			result := client.RunProducer(id, topic, filePath, numData-1)
			pMu.Lock()
			producerResults = append(producerResults, result)
			pMu.Unlock()
		}(i)
	}

	wg.Wait()

	return producerResults, consumerResults
}

func test(topic string, numProducer int, numConsumer int, numData int, target string) {

	kafkaHost := "121.141.225.28"
	brokerHost := "121.141.225.28"
	var brokerPort uint = 11010

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	testFilePath := path + "/test-dataset.tsv"
	timeout := 30000 * time.Millisecond

	if target == "kf" {
		// Test Kafka
		kafkaBenchClient := kafka.NewBenchKafkaClient(kafkaHost, timeout)
		runBenchClient(kafkaBenchClient, "preheat", 1, 10, testFilePath, 10000) // preheat
		kfProducerResults, kfConsumerResults := runBenchClient(kafkaBenchClient, topic, numProducer, numConsumer, testFilePath, numData)
		saveResult(makeResult(numProducer, kfProducerResults), path+"/kf-producer-result.txt")
		saveResult(makeResult(numConsumer, kfConsumerResults), path+"/kf-consumer-result.txt")
	} else {
		// Test ShapleQ
		shapleQBenchClient := shapleQ.NewBenchShapleQClient(brokerHost, brokerPort, timeout)
		runBenchClient(shapleQBenchClient, "preheat", 1, 10, testFilePath, 10000) // preheat
		sqProducerResults, sqConsumerResults := runBenchClient(shapleQBenchClient, topic, numProducer, numConsumer, testFilePath, numData)
		saveResult(makeResult(numProducer, sqProducerResults), path+"/sq-producer-result.txt")
		saveResult(makeResult(numConsumer, sqConsumerResults), path+"/sq-consumer-result.txt")
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

	test(topic, numProducer, numConsumer, numData, testTarget)
}
