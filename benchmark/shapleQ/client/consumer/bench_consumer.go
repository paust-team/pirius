package main

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("Usage: ./sq-consumer-bench [topic-name] [total-data-count]")
	}
	topicName := os.Args[1]
	totalCount, err := strconv.Atoi(os.Args[2])

	if err != nil {
		log.Fatal(err)
	}

	configPath := "../config.yml"

	consumerConfig := config.NewConsumerConfig()
	consumerConfig.Load(configPath)
	consumer := client.NewConsumer(consumerConfig, topicName)

	if err := consumer.Connect(); err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	subscribeCh, errCh, err := consumer.Subscribe(0)
	if err != nil {
		log.Fatal(err)
	}

	receivedCount := 0
	for {
		select {
		case _, ok := <-subscribeCh:
			if ok {
				receivedCount++
				if totalCount == receivedCount {
					endTimestamp := time.Now().UnixNano() / 1000000
					fmt.Println(endTimestamp)
					return
				}
			} else {
				log.Fatalln("not enough data")
			}

		case err := <-errCh:
			log.Fatal(err)
		}
	}
}
