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

	var actual [][]byte
	var expected [][]byte
	var count = 100000

	for i := 0; i < count; i++ {
		expected = append(expected, []byte(strconv.Itoa(i)))
	}

	go func() {
		conn := <-serverConnCh
		defer conn.Close()
		defer close(readDone)

		sock := NewSocket(conn, 5000, 5000)
		msgCh, errCh := sock.continuousRead(ctx)
		close(readStarted)

		for {
			select {
			case msg := <-msgCh:
				if msg != nil {
					actual = append(actual, msg.Data)
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
	writeErrCh := sock.continuousWrite(ctx, msgCh)

	<-readStarted
	for _, msg := range expected {
		msgCh <- message.NewQMessage(message.STREAM, msg)
	}
	select {
	case err := <-writeErrCh:
		t.Fatal(err)
	case <-readDone:
	}

	for i := 0; i < len(expected); i++ {
		if bytes.Compare(actual[i], expected[i]) != 0 {
			t.Errorf("received message is different from sent message. actual: %s, expected: %s", actual[i], expected[i])
		}
	}
}
