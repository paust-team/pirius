package test

import (
	"context"
	"github.com/elon0823/paustq/client"
	"testing"
)

func TestClient_Create(t *testing.T) {
	host := "127.0.0.1:3000"
	ctx := context.Background()
	c := client.NewClient(ctx, host, 5, 0)

	if c.HostUrl != host {
		t.Error("Host not matching")
	}
}

