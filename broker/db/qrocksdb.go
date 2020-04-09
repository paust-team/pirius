package db

import (
	"encoding/binary"
	"github.com/tecbot/gorocksdb"
	"log"
	"path/filepath"
	"unsafe"
)

type CFIndex int

const (
	DefaultCF= iota
	TopicCF
	RecordCF
)

// QRocksDB is helper for gorocksdb
type QRocksDB struct {
	db 					*gorocksdb.DB
	ro                  *gorocksdb.ReadOptions
	wo                  *gorocksdb.WriteOptions
	columnFamilyHandles gorocksdb.ColumnFamilyHandles
}

func NewRocksDB(name, dir string) (*QRocksDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	columnFamilyNames := []string{"default", "topic", "record"}

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(1 << 30))
	defaultOpts := gorocksdb.NewDefaultOptions()
	defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)

	opts := gorocksdb.NewDefaultOptions()
	db, columnFamilyHandles, err := gorocksdb.OpenDbColumnFamilies(defaultOpts, dbPath, columnFamilyNames, []*gorocksdb.Options{opts, opts, opts})

	if err != nil {
		log.Fatal("DB open error: ", err)
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	rocksdb := &QRocksDB{db: db, ro: ro, wo: wo, columnFamilyHandles: columnFamilyHandles}
	return rocksdb, nil
}

func (db *QRocksDB) GetRecord(topic string, offset uint64) (*gorocksdb.Slice, error) {
	key := NewRecordKey(topic, offset)
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[RecordCF], key.Bytes())
}

func (db *QRocksDB) PutRecord(topic string, offset uint64, data []byte) error {
	key := NewRecordKey(topic, offset)
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Bytes(), data)
}

func (db *QRocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
}

func (db *QRocksDB) ColumnFamilyHandles() gorocksdb.ColumnFamilyHandles {
	return db.columnFamilyHandles
}

func (db *QRocksDB) Scan(cfIndex CFIndex) *gorocksdb.Iterator {
	return db.db.NewIteratorCF(db.ro, db.ColumnFamilyHandles()[cfIndex])
}

type RecordKey struct {
	bytes 	[]byte
}

func NewRecordKey(topic string, offset uint64) *RecordKey {
	storage := make([]byte, len(topic) + 1 + int(unsafe.Sizeof(offset)))
	copy(storage, topic+"@")
	binary.BigEndian.PutUint64(storage[:len(topic)+1], offset)

	return &RecordKey{bytes: storage}
}

func (key RecordKey) Bytes() []byte {
	return key.bytes
}

func (key RecordKey) Topic() string {
	return string(key.bytes[:len(key.bytes) - int(unsafe.Sizeof(uint64(0)))])
}

func (key RecordKey) Offset() uint64 {
	return binary.BigEndian.Uint64(key.bytes[len(key.bytes) - int(unsafe.Sizeof(uint64(0))):])
}

