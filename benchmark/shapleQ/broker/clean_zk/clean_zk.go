package main

import (
	"github.com/paust-team/shapleq/broker/config"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"log"
)

func main() {
	brokerConfig := config.NewBrokerConfig()
	coordiWrapper := coordinator_helper.NewCoordinatorWrapper(brokerConfig.ZKQuorum(), 3000, 0, nil)
	if err := coordiWrapper.Connect(); err != nil {
		log.Panic(err)
	}

	coordiWrapper.RemoveAllPath()
	coordiWrapper.Close()
}
