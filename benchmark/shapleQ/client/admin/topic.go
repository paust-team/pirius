package main

import (
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"log"
	"os"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("Usage: ./sq-topic [create|delete] [topic-name]")
	}
	command := os.Args[1]
	topicName := os.Args[2]

	configPath := "../config.yml"

	adminConfig := config.NewAdminConfig()
	adminConfig.Load(configPath)
	adminClient := client.NewAdmin(adminConfig)

	defer adminClient.Close()

	if err := adminClient.Connect(); err != nil {
		log.Fatalln(err)
	}

	if command == "create" {
		if err := adminClient.CreateTopic(topicName, "", 1, 1); err != nil {
			log.Fatalln(err)
		}
	} else {
		if err := adminClient.DeleteTopic(topicName); err != nil {
			log.Fatalln(err)
		}
	}

	fmt.Println("ok")
}
