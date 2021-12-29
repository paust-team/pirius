package message

import (
	"bytes"
	shapleqproto "github.com/paust-team/shapleq/proto"
	"testing"
)

func TestQMessage(t *testing.T) {

	testByte := []byte{1, 2, 3, 4, 5}
	testNodeId := "test"
	var testSeqNum uint64 = 0

	msg, err := NewQMessageFromMsg(STREAM, NewPutRequestMsg(testByte, testSeqNum, testNodeId))
	if err != nil {
		t.Error(err)
	}

	putMsg, err := msg.UnpackTo(&shapleqproto.PutRequest{})
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(testByte, putMsg.(*shapleqproto.PutRequest).Data) != 0 {
		t.Error("bytes not equal")
	}
}
