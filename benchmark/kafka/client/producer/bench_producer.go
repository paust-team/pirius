package main

import (
	"encoding/csv"
	"fmt"
	"github.com/paust-team/shapleq/client/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {

	topicName := ""
	filePath := "../../../testset.tsv"
	numDataCount := 1

	argc := len(os.Args)

	switch argc {
	case 2:
		topicName = os.Args[1]
	case 3:
		topicName = os.Args[1]
		filePath = os.Args[2]
	case 4:
		topicName = os.Args[1]
		filePath = os.Args[2]
		count, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Fatal(err)
		}
		numDataCount = count
	default:
		log.Fatal("Usage: ./kf-producer-bench [topic-name] [file-path:optional] [num-data-count]")
	}

	configPath := "../config.yml"

	clientConfig := config.NewClientConfigBase()
	clientConfig.Load(configPath)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": clientConfig.GetString("bootstrap.servers")})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	testFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(testFile)
	}
	defer testFile.Close()

	reader := csv.NewReader(testFile)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()

	deliveryChan := make(chan kafka.Event)
	startTimestamp := time.Now().UnixNano() / 1000000

	for i, record := range records {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Value:          []byte(record[0]),
		}, deliveryChan)

		if err != nil {
			log.Fatal(err)
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.Fatal(m.TopicPartition.Error)
		}

		if i == numDataCount {
			break
		}
	}

	fmt.Println(startTimestamp)
}
