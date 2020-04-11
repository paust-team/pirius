package test

import (
	"bytes"
	"github.com/paust-team/paustq/broker/storage"
	"testing"
)

func TestRecordKey(t *testing.T) {

	topic := "test_topic"
	var offset uint64 = 1

	key := storage.NewRecordKey(topic, offset)

	if key.Topic() != topic {
		t.Error("Unknown topic")
	}
	if key.Offset() != offset {
		t.Error("Unknown offset")
	}
}

func TestTopicValue(t *testing.T) {

	topicMeta := "test"
	var numPartitions uint32 = 2
	var replicationFactor uint32 = 3

	key := storage.NewTopicValue(topicMeta, numPartitions, replicationFactor)

	if key.TopicMeta() != topicMeta {
		t.Error("Unknown topicMeta")
	}
	if key.NumPartitions() != numPartitions {
		t.Error("Unknown numPartitions")
	}
	if key.ReplicationFactor() != replicationFactor {
		t.Error("Unknown replicationFactor")
	}
}


func TestQRocksDBRecord(t *testing.T) {

	db, err := storage.NewQRocksDB("qstore", ".")
	defer db.Close()

	if err != nil {
		t.Error(err)
		return
	}

	expected := []byte{1,2,3,4,5}
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