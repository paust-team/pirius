package storage

import (
	"encoding/binary"
	"errors"
	"github.com/tecbot/gorocksdb"
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
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetTailing(true)
	wo := gorocksdb.NewDefaultWriteOptions()
	return &QRocksDB{dbPath: dbPath, db: db, ro: ro, wo: wo, columnFamilyHandles: columnFamilyHandles}, nil
}

func (db QRocksDB) Flush() error {
	return db.db.Flush(&gorocksdb.FlushOptions{})
}

func (db QRocksDB) GetRecord(topic string, offset uint64) (*gorocksdb.Slice, error) {
	key := NewRecordKeyFromData(topic, offset)
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[RecordCF], key.Data())
}

func (db QRocksDB) PutRecord(topic string, offset uint64, nodeId string, seqNum uint64, data []byte) error {
	if len(nodeId) != 32 {
		return errors.New("invalid length for node id")
	}
	key := NewRecordKeyFromData(topic, offset)
	value := NewRecordValueFromData(nodeId, seqNum, data)

	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data(), value.Data())
}

func (db QRocksDB) DeleteRecord(topic string, offset uint64) error {
	key := NewRecordKeyFromData(topic, offset)
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data())
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

func (k RecordKey) Data() []byte {
	if k.isSlice {
		return k.Slice.Data()
	}
	return k.data
}

func (k *RecordKey) SetData(data []byte) {
	copy(k.data, data)
}

func (k RecordKey) Size() int {
	if k.isSlice {
		return k.Slice.Size()
	}
	return len(k.data)
}

func (k RecordKey) Topic() string {
	return string(k.Data()[:k.Size()-int(unsafe.Sizeof(uint64(0)))-1])
}

func (k RecordKey) Offset() uint64 {
	return binary.BigEndian.Uint64(k.Data()[k.Size()-int(unsafe.Sizeof(uint64(0))):])
}

type RecordValue struct {
	*gorocksdb.Slice
	data    []byte
	isSlice bool
}

func NewRecordValueFromData(nodeId string, seqNum uint64, publishedData []byte) *RecordValue {
	meta := make([]byte, len(nodeId)+int(unsafe.Sizeof(seqNum)))
	copy(meta, nodeId)
	binary.BigEndian.PutUint64(meta[len(nodeId):], seqNum)

	data := append(meta[:], publishedData[:]...)
	return &RecordValue{data: data, isSlice: false}
}

func NewRecordValue(slice *gorocksdb.Slice) *RecordValue {
	return &RecordValue{Slice: slice, isSlice: true}
}

func (v RecordValue) Data() []byte {
	if v.isSlice {
		return v.Slice.Data()
	}
	return v.data
}

func (v *RecordValue) SetData(data []byte) {
	copy(v.data, data)
}

func (v RecordValue) Size() int {
	if v.isSlice {
		return v.Slice.Size()
	}
	return len(v.data)
}

func (v RecordValue) NodeId() string {
	return string(v.Data()[:32])
}

func (v RecordValue) SeqNum() uint64 {
	return binary.BigEndian.Uint64(v.Data()[32 : 32+int(unsafe.Sizeof(uint64(0)))])
}

func (v RecordValue) PublishedData() []byte {
	return v.Data()[32+int(unsafe.Sizeof(uint64(0))):]
}
