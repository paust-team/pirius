package network

import (
	"bytes"
	"context"
	"github.com/paust-team/shapleq/message"
	"net"
	"strconv"
	"testing"
)

func TestSocket_ContinuousReadWrite(t *testing.T) {
	ctx := context.Background()

	listener, err := net.Listen("tcp", ":1101")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverConnCh := make(chan net.Conn)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		serverConnCh <- conn
	}()

	readStarted := make(chan bool)
	readDone := make(chan bool)

	var actual []*message.QMessage
	var expected []*message.QMessage
	var count = 1000

	for i := 0; i < count; i++ {
		msg, _ := message.NewQMessageFromMsg(message.STREAM, message.NewAckMsg(uint32(i), "msg-"+strconv.Itoa(i)))
		expected = append(expected, msg)
	}

	go func() {
		conn := <-serverConnCh
		defer conn.Close()
		defer close(readDone)

		sock := NewSocket(conn, 5000, 5000)
		msgCh, errCh := sock.ContinuousRead(ctx)
		close(readStarted)

		for {
			select {
			case msg := <-msgCh:
				if msg != nil {
					actual = append(actual, msg)
					if len(actual) == len(expected) {
						return
					}
				}

			case err := <-errCh:
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}()

	conn, err := net.Dial("tcp", ":1101")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	msgCh := make(chan *message.QMessage)
	sock := NewSocket(conn, 5000, 5000)
	writeErrCh := sock.ContinuousWrite(ctx, msgCh)

	<-readStarted
	for _, msg := range expected {
		msgCh <- msg
	}
	select {
	case err := <-writeErrCh:
		t.Fatal(err)
	case <-readDone:
	}

	for i := 0; i < len(expected); i++ {
		found := false
		for j := 0; j < len(actual); j++ {
			if bytes.Compare(actual[j].Data, expected[i].Data) == 0 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected message is not received. expected: %s", expected[i].Data)
		}
	}
}
