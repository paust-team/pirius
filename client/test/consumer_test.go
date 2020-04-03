package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elon0823/paustq/client/consumer"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"testing"
)

func handleSubscribeMessage(serverReceiveChannel chan []byte, serverSendChannel chan []byte, testData map[string][][]byte) {

	for received := range serverReceiveChannel {
		fetchReqMsg := &paustq_proto.FetchRequest{}

		if err := message.UnPackTo(received, fetchReqMsg); err != nil {
			continue
		}

		if testData[fetchReqMsg.TopicName] != nil {
			topicData := testData[fetchReqMsg.TopicName]

			for _, record := range topicData {
				fetchResMsg, err := message.NewFetchResponseMsg(record, 0)
				if err != nil {
					fmt.Println("Failed to create FetchResponse message")
					continue
				}
				serverSendChannel <- fetchResMsg
			}

			fetchEnd, err := message.NewFetchResponseMsg(nil, 1)
			if err != nil {
				fmt.Println("Failed to create FetchResponse message")
				continue
			}

			serverSendChannel <- fetchEnd
		}
	}
}

func TestConsumerSubscribe(t *testing.T) {

	testRecordMap := map[string][][]byte{
		"topic1": {{'g', 'o', 'o', 'g', 'l', 'e'}, {'p', 'a', 'u', 's', 't', 'q'}, {'1', '2', '3', '4', '5', '6'}},
		"topic2": {{'t', 'e', 's', 't'}},
	}

	// Setup test tcp server
	serverReceiveChannel := make(chan []byte)
	serverSendChannel := make(chan []byte)

	server := NewTcpServer(":8001", serverReceiveChannel, serverSendChannel)
	err := server.StartListen()
	if err != nil {
		t.Error(err)
		return
	}

	go handleSubscribeMessage(serverReceiveChannel, serverSendChannel, testRecordMap)

	defer server.Stop()

	host := "127.0.0.1:8001"
	ctx := context.Background()

	client := consumer.NewConsumer(ctx, host, 5)
	if client.Connect() != nil {
		t.Error("Error on connect")
	}

	var receivedRecords [][]byte
	for response := range client.Subscribe("topic1") {
		if response.Error != nil {
			t.Error(response.Error)
		} else {
			receivedRecords = append(receivedRecords, response.Data)
		}
	}

	if len(testRecordMap["topic1"]) != len(receivedRecords) {
		t.Error("Length Mismatch - Send records: ", len(testRecordMap["topic1"]), ", Received records: ", len(receivedRecords))
	}
	for i, record := range testRecordMap["topic1"] {
		if bytes.Compare(receivedRecords[i], record) != 0 {
			t.Error("Record is not same")
		}
	}

	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}
