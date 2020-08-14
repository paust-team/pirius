package storage

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"
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

func TestQRocksDBTailingIterator(t *testing.T) {

	db, err := NewQRocksDB("qstore", ".")

	if err != nil {
		t.Error(err)
		return
	}

	defer db.Destroy()
	defer db.Close()

	topic := "test_topic"

	var putInt = func(offset uint64, b []byte) {
		if db.PutRecord(topic, offset, b) != nil {
			t.Fatal(err)
		}
	}
	putInt(0, []byte{1})

	it := db.Scan(RecordCF)
	keyData := NewRecordKeyFromData(topic, 0).Data()
	it.Seek(keyData)
	prefixData := []byte(topic + "@")

	expectedByte := []byte{2}
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()
		for {
			if it.ValidForPrefix(prefixData) {
				if bytes.Compare(it.Value().Data(), expectedByte) == 0 {
					return
				}
			} else {
				it.Seek(keyData)
			}
			it.Next()
			runtime.Gosched()
		}
	}()

	go func() {
		time.Sleep(time.Second * 1)
		putInt(1, expectedByte)
	}()

	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Second * 10):
		t.Error("timeout..")
	}

}
