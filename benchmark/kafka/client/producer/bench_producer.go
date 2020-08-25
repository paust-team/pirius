package main

import (
	"bytes"
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
			log.Fatalln(err)
		}
		numDataCount = count
	default:
		log.Fatalln("Usage: ./kf-producer-bench [topic-name] [file-path:optional] [num-data-count]")
	}

	configPath := "../config.yml"

	clientConfig := config.NewClientConfigBase()
	clientConfig.Load(configPath)

	p, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers":                     clientConfig.GetString("bootstrap.servers"),
			"batch.size":                            1,
			"linger.ms":                             0,
			"acks":                                  1,
			"retries":                               5,
			"max.in.flight.requests.per.connection": 1,
			"request.timeout.ms":                    clientConfig.Timeout(),
		})
	if err != nil {
		log.Fatalln(err)
	}

	defer p.Close()

	testFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(testFile)
	}
	defer testFile.Close()

	reader := csv.NewReader(testFile)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()

	deliveryChan := make(chan kafka.Event)
	startTimestamp := time.Now().UnixNano() / 1000000

	for i, record := range records {
		sendRecord := []byte(record[0])
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Value:          sendRecord,
		}, deliveryChan)

		if err != nil {
			log.Fatalln(err)
		}

		event := <-deliveryChan
		m := event.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.Fatal(m.TopicPartition.Error)
		}

		if bytes.Compare(m.Value, sendRecord) != 0 {
			log.Fatalln("record not matched")
		}

		if i == numDataCount {
			break
		}
	}

	fmt.Println(startTimestamp - time.Now().UnixNano()/1000000)
}
