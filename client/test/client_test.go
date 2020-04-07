package test

import (
	"context"
	"fmt"
	"github.com/elon0823/paustq/client"
	"testing"
	"time"
)

type RecordMap map[string][][]byte
type MockMessageHandler func(chan TopicData, chan []byte, RecordMap)

func StartTestServer(port string, mockMsgHandler MockMessageHandler, testRecordMap RecordMap) (*TcpServer, error) {

	recvCh := make(chan TopicData)
	sendCh := make(chan []byte)

	server := NewTcpServer(port, recvCh, sendCh)
	err := server.StartListen()
	if err != nil {
		return nil, err
	}

	if mockMsgHandler != nil {
		go mockMsgHandler(recvCh, sendCh, testRecordMap)
	}

	return server, nil
}

func TestClient_Connect(t *testing.T) {

	ip := "127.0.0.1"
	port := ":3000"
	timeout := 5
	host := fmt.Sprintf("%s%s", ip, port)
	ctx := context.Background()
	topic := "test_topic1"

	// Start Server
	server, err := StartTestServer(port, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}

	defer server.Stop()

	// Start Client
	c := client.NewClient(ctx, host, time.Duration(timeout), 0)

	if c.Connect(topic) != nil {
		t.Error("Error on connect")
		return
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}
}
