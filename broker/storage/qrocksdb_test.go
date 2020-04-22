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

func TestTopicValue(t *testing.T) {

	topicMeta := "test"
	var numPartitions uint32 = 2
	var replicationFactor uint32 = 3

	key := NewTopicValueFromData(topicMeta, numPartitions, replicationFactor)

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

func TestQRocksDBTopic(t *testing.T) {

	db, err := NewQRocksDB("qstore", ".")

	if err != nil {
		t.Error(err)
		return
	}

	defer db.Destroy()
	defer db.Close()

	topic := "test_topic"
	topic2 := "test_topic2"
	topicMeta := "test"
	var numPartitions uint32 = 2
	var replicationFactor uint32 = 3

	expected := NewTopicValueFromData(topicMeta, numPartitions, replicationFactor)

	if db.PutTopic(topic, topicMeta, numPartitions, replicationFactor) != nil {
		t.Error(err)
		return
	}

	result, err := db.GetTopic(topic)
	if err != nil {
		t.Error(err)
		return
	}

	if !result.Exists() {
		t.Error("Topic not exists")
		return
	}

	if db.PutTopicIfNotExists(topic, topicMeta, numPartitions, replicationFactor) == nil {
		t.Error("Should return error on PutTopicIfNotExists")
		return
	}

	topicValue := NewTopicValue(result)

	if bytes.Compare(expected.Data(), topicValue.Data()) != 0 {
		t.Error("topic value not equal ")
	}

	if err = db.PutTopicIfNotExists(topic2, topicMeta, numPartitions, replicationFactor); err != nil {
		t.Error(err)
		return
	}

	topics := db.GetAllTopics()
	if len(topics) != 2 {
		t.Error("Topics not stored correctly")
		return
	}

	if err = db.DeleteTopic(topic); err != nil {
		t.Error(err)
		return
	}

	if err = db.DeleteTopic(topic); err != nil {
		t.Error(err)
		return
	}
	
	result, err = db.GetTopic(topic)
	if err != nil {
		t.Error(err)
		return
	}

	if result.Exists() {
		t.Error("Topic should not exists")
		return
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
