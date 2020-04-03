package test

import (
	"context"
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
			continue
		}

		putResMsg, err := message.NewPutResponseMsg(putReqMsg.TopicName, 0)
		if err != nil {
			fmt.Println("Failed to create PutResponse message")
			continue
		}
		serverSendChannel <- putResMsg
	}
}

func TestProducerPublish(t *testing.T) {

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
	testRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	topics := []string{"topic1", "topic2", "topic3"}
	var receivedTopics []string
	var waitGroup sync.WaitGroup

	onResponse := make(chan paustq_proto.PutResponse)
	host := "127.0.0.1:8000"
	ctx := context.Background()

	client := producer.NewProducer(ctx, host, 5).WithOnReceiveResponse(onResponse)
	if client.Connect() != nil {
		t.Error("Error on connect")
	}

	go func() {
		for response := range onResponse {
			if response.ErrorCode != 0 {
				t.Error("PutResponse has error code: ", response.ErrorCode)
			} else {
				receivedTopics = append(receivedTopics, response.TopicName)
			}
			waitGroup.Done()
		}
	}()

	for i, record := range testRecords {
		waitGroup.Add(1)
		client.Publish(topics[i], record)
	}

	waitGroup.Wait()
	if len(topics) != len(receivedTopics) {
		t.Error("Length Mismatch - Send records: ", len(topics), ", Received records: ", len(receivedTopics))
	}
	for _, topic := range topics {
		if !contains(receivedTopics, topic) {
			t.Error("Topic not exists in received topics")
			break
		}
	}

	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}
