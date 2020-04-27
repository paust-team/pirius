package storage

import (
	"encoding/binary"
	"errors"
	"github.com/tecbot/gorocksdb"
	"log"
	"path/filepath"
	"unsafe"
)

type CFIndex int

const (
	DefaultCF = iota
	TopicCF
	RecordCF
)

// QRocksDB is helper for gorocksdb
type QRocksDB struct {
	dbPath              string
	db                  *gorocksdb.DB
	ro                  *gorocksdb.ReadOptions
	wo                  *gorocksdb.WriteOptions
	columnFamilyHandles gorocksdb.ColumnFamilyHandles
}

func NewQRocksDB(name, dir string) (*QRocksDB, error) {

	dbPath := filepath.Join(dir, name+".dbstorage")
	columnFamilyNames := []string{"default", "topic", "record"}

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(1 << 30))
	defaultOpts := gorocksdb.NewDefaultOptions()
	defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)
	defaultOpts.SetCompression(gorocksdb.SnappyCompression)
	opts := gorocksdb.NewDefaultOptions()
	db, columnFamilyHandles, err := gorocksdb.OpenDbColumnFamilies(defaultOpts, dbPath, columnFamilyNames, []*gorocksdb.Options{opts, opts, opts})
	if err != nil {
		log.Fatal("DB open error: ", err)
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	rocksdb := &QRocksDB{dbPath: dbPath, db: db, ro: ro, wo: wo, columnFamilyHandles: columnFamilyHandles}
	return rocksdb, nil
}

func (db QRocksDB) Flush() error {
	return db.db.Flush(&gorocksdb.FlushOptions{})
}

func (db QRocksDB) GetRecord(topic string, offset uint64) (*gorocksdb.Slice, error) {
	key := NewRecordKeyFromData(topic, offset)
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[RecordCF], key.Data())
}

func (db QRocksDB) PutRecord(topic string, offset uint64, data []byte) error {
	key := NewRecordKeyFromData(topic, offset)
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data(), data)
}

func (db QRocksDB) DeleteRecord(topic string, offset uint64) error {
	key := NewRecordKeyFromData(topic, offset)
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data())
}

func (db QRocksDB) PutTopicIfNotExists(topic string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {

	result, err := db.GetTopic(topic)

	if err != nil {
		return err
	}

	if result != nil && result.Exists() {
		return errors.New("topic already exists")
	}

	if err := db.PutTopic(topic, topicMeta, numPartitions, replicationFactor); err != nil {
		return err
	}
	return nil
}

func (db QRocksDB) PutTopic(topic string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {
	value := NewTopicValueFromData(topicMeta, numPartitions, replicationFactor)
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[TopicCF], []byte(topic), value.Data())
}

func (db QRocksDB) GetTopic(topic string) (*gorocksdb.Slice, error) {
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[TopicCF], []byte(topic))
}

func (db QRocksDB) DeleteTopic(topic string) error {
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[TopicCF], []byte(topic))
}

func (db QRocksDB) GetAllTopics() map[string]TopicValue {
	iter := db.Scan(TopicCF)
	iter.SeekToFirst()
	topics := make(map[string]TopicValue)
	for iter.Valid() {
		topicName := string(iter.Key().Data())
		topicValue := NewTopicValue(iter.Value())
		topics[topicName] = *topicValue
		iter.Next()
	}

	return topics
}

func (db *QRocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
}

func (db *QRocksDB) Destroy() error {
	return gorocksdb.DestroyDb(db.dbPath, gorocksdb.NewDefaultOptions())
}

func (db QRocksDB) ColumnFamilyHandles() gorocksdb.ColumnFamilyHandles {
	return db.columnFamilyHandles
}

func (db QRocksDB) Scan(cfIndex CFIndex) *gorocksdb.Iterator {
	return db.db.NewIteratorCF(db.ro, db.ColumnFamilyHandles()[cfIndex])
}

type RecordKey struct {
	*gorocksdb.Slice
	data    []byte
	isSlice bool
}

func NewRecordKeyFromData(topic string, offset uint64) *RecordKey {
	data := make([]byte, len(topic)+1+int(unsafe.Sizeof(offset)))
	copy(data, topic+"@")
	binary.BigEndian.PutUint64(data[len(topic)+1:], offset)
	return &RecordKey{data: data, isSlice: false}
}

func NewRecordKey(slice *gorocksdb.Slice) *RecordKey {
	return &RecordKey{Slice: slice, isSlice: true}
}

func (key RecordKey) Data() []byte {
	if key.isSlice {
		return key.Slice.Data()
	}
	return key.data
}

func (key *RecordKey) SetData(data []byte) {
	copy(key.data, data)
}

func (key RecordKey) Size() int {
	if key.isSlice {
		return key.Slice.Size()
	}
	return len(key.data)
}

func (key RecordKey) Topic() string {
	return string(key.Data()[:key.Size()-int(unsafe.Sizeof(uint64(0)))-1])
}

func (key RecordKey) Offset() uint64 {
	return binary.BigEndian.Uint64(key.Data()[key.Size()-int(unsafe.Sizeof(uint64(0))):])
}

type TopicValue struct {
	*gorocksdb.Slice
	data    []byte
	isSlice bool
}

func NewTopicValueFromData(topicMeta string, numPartitions uint32, replicationFactor uint32) *TopicValue {
	data := make([]byte, len(topicMeta)+int(unsafe.Sizeof(numPartitions))+int(unsafe.Sizeof(replicationFactor)))
	copy(data, topicMeta)
	binary.BigEndian.PutUint32(data[len(topicMeta):], numPartitions)
	binary.BigEndian.PutUint32(data[len(topicMeta)+int(unsafe.Sizeof(numPartitions)):], replicationFactor)

	return &TopicValue{data: data, isSlice: false}
}

func NewTopicValue(slice *gorocksdb.Slice) *TopicValue {
	return &TopicValue{Slice: slice, isSlice: true}
}

func (key TopicValue) Data() []byte {
	if key.isSlice {
		return key.Slice.Data()
	}
	return key.data
}

func (key TopicValue) Size() int {
	if key.isSlice {
		return key.Slice.Size()
	}
	return len(key.data)
}

func (key TopicValue) TopicMeta() string {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return string(key.Data()[:key.Size()-uint32Len*2])
}

func (key TopicValue) NumPartitions() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len*2 : key.Size()-uint32Len])
}

func (key TopicValue) ReplicationFactor() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.Data()[key.Size()-uint32Len:])
}
