package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/client/consumer"
	"github.com/paust-team/paustq/client/producer"
	paustqproto "github.com/paust-team/paustq/proto"
	"testing"
	"time"
)

func TestClient_Connect(t *testing.T) {

	ip := "127.0.0.1"
	port := ":9000"
	host := fmt.Sprintf("%s%s", ip, port)
	ctx := context.Background()
	topic := "test_topic1"

	c := client.NewStreamClient(ctx, host, paustqproto.SessionType_ADMIN)

	if err := c.ConnectWithTopic(topic); err != nil {
		t.Error("Error on connect. ", err)
		return
	}

	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func TestPubSub(t *testing.T) {

	ip := "127.0.0.1"
	port := ":9000"
	timeout := 5

	testRecordMap := map[string][][]byte{
		"topic1": {
			{'g', 'o', 'o', 'g', 'l', 'e'},
			{'p', 'a', 'u', 's', 't', 'q'},
			{'1', '2', '3', '4', '5', '6'}},
	}
	topic := "topic1"
	receivedRecordMap := make(map[string][][]byte)

	host := fmt.Sprintf("%s%s", ip, port)
	ctx1 := context.Background()
	ctx2 := context.Background()

	// Start producer
	producerClient := producer.NewProducer(ctx1, host, time.Duration(timeout))
	if producerClient.Connect(topic) != nil {
		t.Error("Error on connect")
	}

	for _, record := range testRecordMap[topic] {
		producerClient.Publish(record)
	}

	producerClient.WaitAllPublishResponse()

	if err := producerClient.Close(); err != nil {
		t.Error(err)
	}

	// Start consumer
	consumerClient := consumer.NewConsumer(ctx2, host, time.Duration(timeout))
	if consumerClient.Connect(topic) != nil {
		t.Error("Error on connect")
		return
	}

	for response := range consumerClient.Subscribe(0) {
		if response.Error != nil {
			t.Error(response.Error)
		} else {
			receivedRecordMap[topic] = append(receivedRecordMap[topic], response.Data)
		}
	}

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

	if err := consumerClient.Close(); err != nil {
		t.Error(err)
	}
}

