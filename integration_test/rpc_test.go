package integration_test

import (
	"fmt"
	"github.com/paust-team/paustq/broker"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/common"
	"sync"
	"testing"
)

func TestHeartBeat(t *testing.T) {

	zkAddr := "127.0.0.1"
	brokerAddr := fmt.Sprintf("127.0.0.1:%d", common.DefaultBrokerPort)

	// Start broker
	brokerInstance := broker.NewBroker(zkAddr).WithLogLevel(testLogLevel)
	bwg := sync.WaitGroup{}
	bwg.Add(1)

	defer brokerInstance.Clean()
	defer bwg.Wait()
	defer brokerInstance.Stop()

	go func() {
		defer bwg.Done()
		brokerInstance.Start()
	}()

	Sleep(1)

	adminClient := client.NewAdmin(brokerAddr)

	defer adminClient.Close()

	if err := adminClient.Connect(); err != nil {
		t.Fatal(err)
	}

	pongMsg, err := adminClient.Heartbeat("test", 1)
	if err != nil {
		t.Fatal(pongMsg)
	}
	fmt.Println(pongMsg.Echo)

}
