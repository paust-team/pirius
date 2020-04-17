package storage

import (
	"encoding/binary"
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
	dbPath				string
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

func (db QRocksDB) GetRecord(topic string, offset uint64) (*gorocksdb.Slice, error) {
	key := NewRecordKey(topic, offset)
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[RecordCF], key.Bytes())
}

func (db QRocksDB) PutRecord(topic string, offset uint64, data []byte) error {
	key := NewRecordKey(topic, offset)
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Bytes(), data)
}

func (db QRocksDB) DeleteRecord(topic string, offset uint64) error {
	key := NewRecordKey(topic, offset)
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Bytes())
}

func (db QRocksDB) GetTopic(topic string) (*gorocksdb.Slice, error) {
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[TopicCF], []byte(topic))
}

func (db QRocksDB) PutTopic(topic string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {
	value := NewTopicValue(topicMeta, numPartitions, replicationFactor)
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[TopicCF], []byte(topic), value.Bytes())
}

func (db QRocksDB) DeleteTopic(topic string) error {
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[TopicCF], []byte(topic))
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
	bytes []byte
}

func NewRecordKey(topic string, offset uint64) *RecordKey {
	storage := make([]byte, len(topic)+1+int(unsafe.Sizeof(offset)))
	copy(storage, topic+"@")
	binary.BigEndian.PutUint64(storage[len(topic)+1:], offset)
	return &RecordKey{bytes: storage}
}

func (key RecordKey) Bytes() []byte {
	return key.bytes
}

func (key RecordKey) Topic() string {
	return string(key.bytes[:len(key.bytes)-int(unsafe.Sizeof(uint64(0)))-1])
}

func (key RecordKey) Offset() uint64 {
	return binary.BigEndian.Uint64(key.bytes[len(key.bytes)-int(unsafe.Sizeof(uint64(0))):])
}

type TopicValue struct {
	bytes []byte
}

func NewTopicValue(topicMeta string, numPartitions uint32, replicationFactor uint32) *TopicValue {
	storage := make([]byte, len(topicMeta)+int(unsafe.Sizeof(numPartitions))+int(unsafe.Sizeof(replicationFactor)))
	copy(storage, topicMeta)
	binary.BigEndian.PutUint32(storage[len(topicMeta):], numPartitions)
	binary.BigEndian.PutUint32(storage[len(topicMeta)+int(unsafe.Sizeof(numPartitions)):], replicationFactor)

	return &TopicValue{bytes: storage}
}

func NewTopicValueWithBytes(bytes []byte) *TopicValue {
	return &TopicValue{bytes: bytes}
}

func (key TopicValue) Bytes() []byte {
	return key.bytes
}

func (key TopicValue) TopicMeta() string {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return string(key.bytes[:len(key.bytes)-uint32Len*2])
}

func (key TopicValue) NumPartitions() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.bytes[len(key.bytes)-uint32Len*2 : len(key.bytes)-uint32Len])
}

func (key TopicValue) ReplicationFactor() uint32 {
	uint32Len := int(unsafe.Sizeof(uint32(0)))
	return binary.BigEndian.Uint32(key.bytes[len(key.bytes)-uint32Len:])
}
