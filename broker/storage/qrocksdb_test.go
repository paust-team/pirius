package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
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

func TestRetentionPeriodKey(t *testing.T) {

	expectedTopic := "test_topic"
	var expectedOffset uint64 = 1
	var expectedFragmentId uint32 = 2
	expectedExpirationDate := GetNowTimestamp() + 10

	key := NewRetentionPeriodKeyFromData(NewRecordKeyFromData(expectedTopic, expectedFragmentId, expectedOffset), expectedExpirationDate)

	if key.ExpirationDate() != expectedExpirationDate {
		t.Error("ExpirationDate not matched")
	}
	if key.RecordKey().Topic() != expectedTopic {
		t.Error("Topic not matched")
	}
	if key.RecordKey().Offset() != expectedOffset {
		t.Error("Offset not matched")
	}
	if key.RecordKey().FragmentId() != expectedFragmentId {
		t.Error("FragmentId not matched")
	}
}

func TestQRocksDBRecord(t *testing.T) {

	db, err := NewQRocksDB("qstore", ".")

	if err != nil {
		t.Fatal(err)
	}

	defer db.Destroy()
	defer db.Close()

	expected := []byte{1, 2, 3, 4, 5}
	topic := "test_topic"
	var fragmentId uint32 = 1
	nodeId := "f47ac10b58cc037285670e02b2c3d479"
	var seqNum uint64 = 10
	expirationDate := GetNowTimestamp() + 10
	if db.PutRecord(topic, fragmentId, 0, nodeId, seqNum, expected, expirationDate) != nil {
		t.Fatal(err)
	}

	record, err := db.GetRecord(topic, fragmentId, 0)

	if err != nil {
		t.Fatal(err)
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

func TestIterateRecord(t *testing.T) {
	db, err := NewQRocksDB("qstore", ".")

	if err != nil {
		t.Fatal(err)
	}

	defer db.Destroy()
	defer db.Close()

	topic := "test_topic2"
	nodeId := "f47ac10b58cc037285670e02b2c3d479"
	var fragmentId uint32 = 1
	expirationDate := GetNowTimestamp() + 10
	prevNumOffset := 100000
	numOffset := 1000
	for i := 0; i < prevNumOffset; i++ {
		if db.PutRecord(topic, fragmentId, uint64(i), nodeId, 0, []byte(fmt.Sprintf("data%d", i)), expirationDate) != nil {
			t.Fatal(err)
		}
	}

	var receivedData [][]byte
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := prevNumOffset; i < prevNumOffset+numOffset; i++ {
			if db.PutRecord(topic, fragmentId, uint64(i), nodeId, 0, []byte(fmt.Sprintf("data%d", i)), expirationDate) != nil {
				t.Error(err)
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	var startOffset = uint64(prevNumOffset)
	it := db.Scan(RecordCF)
	wg.Add(1)
	prefix := make([]byte, len(topic)+1+int(unsafe.Sizeof(uint32(0))))
	copy(prefix, topic+"@")
	binary.BigEndian.PutUint32(prefix[len(topic)+1:], fragmentId)

	go func() {
		defer wg.Done()
		iterateInterval := time.Millisecond * 10
		timer := time.NewTimer(iterateInterval)
		defer timer.Stop()
		prevKey := NewRecordKeyFromData(topic, fragmentId, startOffset)
		for {
			for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), prefix); it.Next() {
				key := NewRecordKey(it.Key())
				if key.Offset() != startOffset {
					break
				}
				value := NewRecordValue(it.Value())
				receivedData = append(receivedData, value.PublishedData())
				startOffset++
				prevKey.SetOffset(startOffset)
				runtime.Gosched()
			}
			if len(receivedData) == numOffset {
				break
			}
			// wait for iterator to be updated
			time.Sleep(1 * time.Second)
		}
	}()
	wg.Wait()

	if len(receivedData) != numOffset {
		t.Errorf("received record length = %d, actual = %d", len(receivedData), numOffset)
	}
}

func TestDeleteExpiredRecords(t *testing.T) {
	db, err := NewQRocksDB("qstore", ".")

	if err != nil {
		t.Fatal(err)
	}

	defer db.Destroy()
	defer db.Close()

	expected := []byte{1, 2, 3, 4, 5}
	topic := "test_topic"
	var fragmentId uint32 = 1
	nodeId := "f47ac10b58cc037285670e02b2c3d479"
	var seqNum uint64 = 10
	shortExpirationDate := GetNowTimestamp() + 1
	longExpirationDate := GetNowTimestamp() + 10
	var offsetShouldBeRemained uint64 = 0
	var offsetShouldBeDeleted uint64 = 1

	if db.PutRecord(topic, fragmentId, offsetShouldBeRemained, nodeId, seqNum, expected, longExpirationDate) != nil {
		t.Fatal(err)
	}

	if db.PutRecord(topic, fragmentId, offsetShouldBeDeleted, nodeId, seqNum, expected, shortExpirationDate) != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	deletedCount, err := db.DeleteExpiredRecords()
	if err != nil {
		t.Error(err)
	}

	if deletedCount != 1 {
		t.Errorf("Deleted count should be %d but got %d", 1, deletedCount)
	}

	record, err := db.GetRecord(topic, fragmentId, offsetShouldBeRemained)
	if err != nil {
		t.Error(err)
	}
	if record.Size() == 0 {
		t.Errorf("record(%d) should be exists", offsetShouldBeRemained)
	}
	record, err = db.GetRecord(topic, fragmentId, offsetShouldBeDeleted)
	if err != nil {
		t.Error(err)
	}
	if record.Size() != 0 {
		t.Errorf("record(%d) should not be exists", offsetShouldBeDeleted)
	}
}
