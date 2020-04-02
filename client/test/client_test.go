package test

import (
	"context"
	"github.com/elon0823/paustq/client"
	"testing"
)

func TestClient_Create(t *testing.T) {

	server := NewTcpServer(":3000", nil, nil)
	err := server.StartListen()
	if err != nil {
		t.Error(err)
		return
	}

	defer server.Stop()

	host := "127.0.0.1:3000"
	ctx := context.Background()
	c := client.NewClient(ctx, host, 5, 0)

	if c.Connect() != nil {
		t.Error("Error on connect")
	}
	err = c.Close()
	if err != nil {
		t.Error(err)
	}
}
