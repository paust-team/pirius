package main

import (
	"bufio"
	"flag"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"log"
	"os"
	"sync"
)

func main() {

	configPath := flag.String("config", "../config.yml", "config path")
	flag.Parse()

	producerConfig := config.NewProducerConfig()
	producerConfig.Load(*configPath)
	producer := client.NewProducer(producerConfig, producerConfig.GetString("benchmark.topic"))

	totalCount := producerConfig.GetInt("benchmark.data-count")
	filePath := producerConfig.GetString("benchmark.file-path")

	if err := producer.Connect(); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	publishCh := make(chan []byte)
	partitionCh, errCh, err := producer.AsyncPublish(publishCh)

	if err != nil {
		log.Fatal(err)
	}
	defer close(publishCh)

	wg := sync.WaitGroup{}
	wg.Add(totalCount)

	receivedCount := 0
	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-partitionCh:
				if ok {
					receivedCount++
					if totalCount == receivedCount {
						log.Println("consumer finished")
						return
					} else {
						log.Fatalln("not enough data")
					}
				}
			}
		}
	}()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		select {
		case publishCh <- []byte(scanner.Text()):

		case err := <-errCh:
			log.Fatal(err)
		}
	}
	wg.Wait()
}
