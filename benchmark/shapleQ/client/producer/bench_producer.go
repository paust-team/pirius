package main

import (
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"log"
	"os"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("Usage: ./sq-producer-bench [topic-name] [data-to-publish]")
	}
	topicName := os.Args[1]
	data := os.Args[2]

	configPath := "../config.yml"

	producerConfig := config.NewProducerConfig()
	producerConfig.Load(configPath)
	producer := client.NewProducer(producerConfig, topicName)

	if err := producer.Connect(); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	_, err := producer.Publish([]byte(data))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("producer finished")
}
