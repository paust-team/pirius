package main

import (
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/zookeeper"
	"log"
)

func main() {
	brokerConfig := config.NewBrokerConfig()
	brokerConfig.Load("broker/config.yml")

	zkClient := zookeeper.NewZKClient(brokerConfig.ZKAddr(), 3)

	defer zkClient.Close()
	defer zkClient.RemoveAllPath()

	if err := zkClient.Connect(); err != nil {
		log.Fatalln(err)
	}
}
