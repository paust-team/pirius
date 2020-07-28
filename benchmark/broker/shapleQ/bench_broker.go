package main

import (
	"flag"
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	configPath := flag.String("config", "../config.yml", "config path")
	flag.Parse()

	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)
	defer close(done)

	brokerConfig := config.NewBrokerConfig()
	brokerConfig.Load(*configPath)
	brokerInstance := broker.NewBroker(brokerConfig)

	go func() {
		brokerInstance.Start()
		done <- true
	}()

	for {
		select {
		case <-done:
			fmt.Println("broker process finished")
			return
		case sig := <-sigCh:
			fmt.Println("received signal:", sig)
			brokerInstance.Stop()
		}
	}
}
