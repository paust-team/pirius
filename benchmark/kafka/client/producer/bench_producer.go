package main

import (
	"github.com/paust-team/shapleq/client/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"sync"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("Usage: ./kf-producer-bench [topic-name] [data-to-publish]")
	}
	topicName := os.Args[1]
	data := os.Args[2]

	configPath := "../config.yml"

	clientConfig := config.NewClientConfigBase()
	clientConfig.Load(configPath)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": clientConfig.GetString("bootstrap.servers")})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	wg := sync.WaitGroup{}
	wg.Add(1)

	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Value:          []byte(data),
	}, deliveryChan)

	if err != nil {
		log.Fatal(err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Fatal(m.TopicPartition.Error)
	}

	log.Println("producer finished")
}
