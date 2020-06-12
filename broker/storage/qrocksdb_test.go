package storage

import (
	"bytes"
	"testing"
)

func TestRecordKey(t *testing.T) {

	topic := "test_topic"
	var offset uint64 = 1

	key := NewRecordKeyFromData(topic, offset)

	if key.Topic() != topic {
		t.Error("Unknown topic")
	}
	if key.Offset() != offset {
		t.Error("Unknown offset")
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
	if db.PutRecord(topic, 0, expected) != nil {
		t.Error(err)
		return
	}

	result, err := db.GetRecord(topic, 0)

	if err != nil {
		t.Error(err)
		return
	}

	if bytes.Compare(expected, result.Data()) != 0 {
		t.Error("Not Equal")
	}
}
