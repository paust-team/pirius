package main

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/client/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"time"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("Usage: ./kf-topic [create|delete] [topic-name]")
	}
	command := os.Args[1]
	topicName := os.Args[2]

	configPath := "../config.yml"

	clientConfig := config.NewClientConfigBase()
	clientConfig.Load(configPath)

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": clientConfig.GetString("bootstrap.servers")})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if command == "create" {
		_, err := a.CreateTopics(
			ctx,

			[]kafka.TopicSpecification{{
				Topic:             topicName,
				NumPartitions:     1,
				ReplicationFactor: 1}},
			// Admin options
			kafka.SetAdminOperationTimeout(time.Duration(clientConfig.Timeout())*time.Millisecond))

		if err != nil {
			log.Fatalln(err)
		}
	} else {
		_, err := a.DeleteTopics(ctx, []string{topicName}, kafka.SetAdminOperationTimeout(time.Duration(clientConfig.Timeout())*time.Millisecond))
		if err != nil {
			log.Fatalln(err)
		}
	}
	fmt.Println("ok")
}
