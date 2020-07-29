package main

import (
	"fmt"
	"github.com/paust-team/shapleq/client/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("Usage: ./kf-consumer-bench [topic-name] [total-data-count]")
	}
	topicName := os.Args[1]
	totalCount, err := strconv.Atoi(os.Args[2])

	if err != nil {
		log.Fatal(err)
	}

	configPath := "../config.yml"

	clientConfig := config.NewClientConfigBase()
	clientConfig.Load(configPath)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     clientConfig.GetString("bootstrap.servers"),
		"broker.address.family": "v4",
		"group.id":              "mygroup",
		"session.timeout.ms":    clientConfig.Timeout(),
		"auto.offset.reset":     "earliest"})

	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	err = c.Subscribe(topicName, nil)

	if err != nil {
		log.Fatalln(err)
	}

	receivedCount := 0
	for {
		_, err := c.ReadMessage(time.Duration(clientConfig.Timeout()) * time.Millisecond)
		if err != nil {
			log.Fatalln(err)
		} else {
			receivedCount++
			fmt.Println("received")
			if totalCount == receivedCount {
				log.Println("consumer finished")
				return
			}
		}
	}
}
