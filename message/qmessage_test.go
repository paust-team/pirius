package message

import (
	"bytes"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"testing"
)

func TestQMessage(t *testing.T) {

	testByte := []byte{1, 2, 3, 4, 5}
	msg, err := NewQMessageFromMsg(STREAM, NewPutRequestMsg(testByte))
	if err != nil {
		t.Error(err)
	}

	if msg.Is(&shapleqproto.FetchRequest{}) {
		t.Error("should be false")
	}

	if !msg.Is(&shapleqproto.PutRequest{}) {
		t.Error("should be true")
	}

	putMsg := &shapleqproto.PutRequest{}
	if err := msg.UnpackTo(putMsg); err != nil {
		t.Error(err)
	}

	if bytes.Compare(testByte, putMsg.Data) != 0 {
		t.Error("bytes not equal")
	}

}
