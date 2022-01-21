package storage

import (
	"bytes"
	"testing"
)

func TestRecordKey(t *testing.T) {

	expectedTopic := "test_topic"
	var expectedOffset uint64 = 1
	var expectedFragmentId uint32 = 2

	key := NewRecordKeyFromData(expectedTopic, expectedFragmentId, expectedOffset)

	if key.Topic() != expectedTopic {
		t.Error("Topic not matched")
	}
	if key.Offset() != expectedOffset {
		t.Error("Offset not matched")
	}
	if key.FragmentId() != expectedFragmentId {
		t.Error("FragmentId not matched")
	}
}

func TestQRocksDBRecord(t *testing.T) {

	db, err := NewQRocksDB("qstore", ".")

	if err != nil {
		t.Error(err)
		return
	}

	defer db.Destroy()
	defer db.Close()

	expected := []byte{1, 2, 3, 4, 5}
	topic := "test_topic"
	var fragmentId uint32 = 1
	nodeId := "f47ac10b58cc037285670e02b2c3d479"
	var seqNum uint64 = 10
	if db.PutRecord(topic, fragmentId, 0, nodeId, seqNum, expected) != nil {
		t.Error(err)
		return
	}

	record, err := db.GetRecord(topic, fragmentId, 0)

	if err != nil {
		t.Error(err)
		return
	}

	value := NewRecordValue(record)
	if bytes.Compare(expected, value.PublishedData()) != 0 {
		t.Error("Published bytes are not Equal")
	}
	if nodeId != value.NodeId() {
		t.Error("Node id is not equal")
	}
	if seqNum != value.SeqNum() {
		t.Error("seq num is not equal")
	}
}
