package main

import (
	"flag"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"log"
)

func main() {

	configPath := flag.String("config", "../config.yml", "config path")
	flag.Parse()

	consumerConfig := config.NewConsumerConfig()
	consumerConfig.Load(*configPath)
	consumer := client.NewConsumer(consumerConfig, consumerConfig.GetString("benchmark.topic"))

	totalCount := consumerConfig.GetInt("benchmark.data-count")

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
					log.Println("consumer finished")
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
