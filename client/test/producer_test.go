package test

import (
	"fmt"
	"github.com/elon0823/paustq/client/producer"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"sync"
	"testing"
)

func handlePublishMessage(serverReceiveChannel chan []byte, serverSendChannel chan []byte) {

	for received := range serverReceiveChannel {
		putReqMsg := &paustq_proto.PutRequest{}

		if err := message.UnPackTo(received, putReqMsg); err != nil {
			fmt.Println(err)
			continue
		}

		putResMsg, err := message.NewPutResponseMsg(putReqMsg.TopicName, 0)
		if err != nil {
			fmt.Println("Failed to create PutRequest message")
			continue
		}
		serverSendChannel <- putResMsg
	}
}

func TestClient_Publish(t *testing.T) {

	// Setup test tcp server
	serverReceiveChannel := make(chan []byte)
	serverSendChannel := make(chan []byte)

	server := NewTcpServer(":8000", serverReceiveChannel, serverSendChannel)
	err := server.StartListen()
	if err != nil {
		t.Error(err)
		return
	}

	go handlePublishMessage(serverReceiveChannel, serverSendChannel)

	defer server.Stop()

	// Test Producer Client
	records := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	topic := "test_topic"
	var waitGroup sync.WaitGroup

	onResponse := make(chan paustq_proto.PutResponse)
	host := "127.0.0.1:8000"
	client := producer.NewProducer(host, 5).WithOnReceiveResponse(onResponse)
	if client.Connect() != nil {
		t.Error("Error on connect")
	}

	go func() {
		for response := range onResponse {
			if response.ErrorCode != 0 {
				t.Error("PutResponse has error code: ", response.ErrorCode)
			}
			waitGroup.Done()
		}
	}()

	for _, record := range records {
		waitGroup.Add(1)
		client.Publish(topic, record)
	}

	waitGroup.Wait()
	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}
