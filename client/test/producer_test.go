package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elon0823/paustq/client/producer"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"testing"
)

func handlePublishMessage(serverReceiveChannel chan []byte, serverSendChannel chan []byte, receivedRecords *[][]byte) {

	for received := range serverReceiveChannel {
		putReqMsg := &paustq_proto.PutRequest{}

		if err := message.UnPackTo(received, putReqMsg); err != nil {
			continue
		}

		putResMsg, err := message.NewPutResponseMsgData(putReqMsg.TopicName, 0)
		if err != nil {
			fmt.Println("Failed to create PutResponse message")
			continue
		}
		*receivedRecords = append(*receivedRecords, putReqMsg.Data)
		serverSendChannel <- putResMsg
	}
}

func TestProducerPublish(t *testing.T) {

	testRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}

	var receivedRecords [][]byte
	topic := "topic1"

	// Setup test tcp server
	serverReceiveChannel := make(chan []byte)
	serverSendChannel := make(chan []byte)

	server := NewTcpServer(":8000", serverReceiveChannel, serverSendChannel)
	err := server.StartListen()
	if err != nil {
		t.Error(err)
		return
	}

	go handlePublishMessage(serverReceiveChannel, serverSendChannel, &receivedRecords)

	defer server.Stop()

	host := "127.0.0.1:8000"
	ctx := context.Background()

	client := producer.NewProducer(ctx, host, 5)
	if client.Connect() != nil {
		t.Error("Error on connect")
	}

	for _, record := range testRecords {
		client.Publish(topic, record)
	}

	client.WaitAllPublishResponse()

	if len(testRecords) != len(receivedRecords) {
		t.Error("Length Mismatch - Send records: ", len(testRecords), ", Received records: ", len(receivedRecords))
	}
	for i, record := range testRecords {
		if bytes.Compare(receivedRecords[i], record) != 0 {
			t.Error("Record is not same")
		}
	}

	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}
