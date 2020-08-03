package main

import (
	"encoding/csv"
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
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
		log.Fatal("Usage: ./sq-producer-bench [topic-name] [file-path:optional] [num-data-count]")
	}

	configPath := "../config.yml"

	producerConfig := config.NewProducerConfig()
	producerConfig.Load(configPath)
	producer := client.NewProducer(producerConfig, topicName)

	if err := producer.Connect(); err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	testFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer testFile.Close()

	reader := csv.NewReader(testFile)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()

	startTimestamp := time.Now().UnixNano() / 1000000
	for i, record := range records {
		_, err = producer.Publish([]byte(record[0]))
		if err != nil {
			log.Fatal(err)
		}
		if i == numDataCount {
			break
		}
	}

	fmt.Println(startTimestamp)
}
