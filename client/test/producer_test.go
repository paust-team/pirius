package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elon0823/paustq/client/producer"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"testing"
	"time"
)

func mockProducerHandler(serverReceiveChannel chan []byte, serverSendChannel chan []byte, receivedRecordMap RecordMap) {

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

		receivedRecordMap[putReqMsg.TopicName] = append(receivedRecordMap[putReqMsg.TopicName], putReqMsg.Data)
		serverSendChannel <- putResMsg
	}
}

func TestProducer_Publish(t *testing.T) {

	ip := "127.0.0.1"
	port := ":8000"
	timeout := 5
	host := fmt.Sprintf("%s%s", ip, port)
	ctx := context.Background()

	testRecordMap := map[string][][]byte{
		"topic1": {
			{'g', 'o', 'o', 'g', 'l', 'e'},
			{'p', 'a', 'u', 's', 't', 'q'},
			{'1', '2', '3', '4', '5', '6'}},
	}
	topic := "topic1"
	receivedRecordMap := make(map[string][][]byte)

	// Start Server
	server, err := StartTestServer(port, mockProducerHandler, receivedRecordMap)
	if err != nil {
		t.Error(err)
		return
	}

	defer server.Stop()

	// Start Client
	client := producer.NewProducer(ctx, host, time.Duration(timeout))
	if client.Connect() != nil {
		t.Error("Error on connect")
	}

	for _, record := range testRecordMap[topic] {
		client.Publish(topic, record)
	}

	client.WaitAllPublishResponse()

	expectedResults := testRecordMap[topic]
	receivedResults := receivedRecordMap[topic]

	if len(expectedResults) != len(receivedResults) {
		t.Error("Length Mismatch - Expected records: ", len(expectedResults), ", Received records: ", len(receivedResults))
	}
	for i, record := range expectedResults {
		if bytes.Compare(receivedResults[i], record) != 0 {
			t.Error("Record is not same")
		}
	}

	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}
