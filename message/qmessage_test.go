package message

import (
	"bytes"
	paustqproto "github.com/paust-team/paustq/proto"
	"testing"
)

func TestQMessage(t *testing.T) {

	testByte := []byte{1, 2, 3, 4, 5}
	msg, err := NewQMessageWithMsg(NewPutRequestMsg(testByte))
	if err != nil {
		t.Error(err)
	}

	if msg.Is(&paustqproto.FetchRequest{}) {
		t.Error("should be false")
	}

	if !msg.Is(&paustqproto.PutRequest{}) {
		t.Error("should be true")
	}

	putMsg := &paustqproto.PutRequest{}
	if err := msg.UnpackTo(putMsg); err != nil {
		t.Error(err)
	}

	if bytes.Compare(testByte, putMsg.Data) != 0 {
		t.Error("bytes not equal")
	}

}