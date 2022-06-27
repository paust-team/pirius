package storage

import (
	"encoding/binary"
	"errors"
	"github.com/linxGnu/grocksdb"
	"path/filepath"
	"unsafe"
)

type CFIndex int

var uint32Len = int(unsafe.Sizeof(uint32(0)))
var uint64Len = int(unsafe.Sizeof(uint64(0)))

const (
	DefaultCF = iota
	TopicCF
	RecordCF
)

// QRocksDB is helper for gorocksdb
type QRocksDB struct {
	dbPath              string
	db                  *grocksdb.DB
	ro                  *grocksdb.ReadOptions
	wo                  *grocksdb.WriteOptions
	columnFamilyHandles grocksdb.ColumnFamilyHandles
}

func NewQRocksDB(name, dir string) (*QRocksDB, error) {

	dbPath := filepath.Join(dir, name)
	columnFamilyNames := []string{"default", "topic", "record"}

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	blockCache := grocksdb.NewLRUCache(1 << 30)
	bbto.SetBlockCache(blockCache)
	defaultOpts := grocksdb.NewDefaultOptions()
	defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)
	defaultOpts.SetCompression(grocksdb.SnappyCompression)
	defaultOpts.SetMaxOpenFiles(16)
	opts := grocksdb.NewDefaultOptions()
	db, columnFamilyHandles, err := grocksdb.OpenDbColumnFamilies(defaultOpts, dbPath, columnFamilyNames, []*grocksdb.Options{opts, opts, opts})
	if err != nil {
		return nil, err
	}

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetTailing(true)
	wo := grocksdb.NewDefaultWriteOptions()
	return &QRocksDB{dbPath: dbPath, db: db, ro: ro, wo: wo, columnFamilyHandles: columnFamilyHandles}, nil
}

func (db QRocksDB) Flush() error {
	return db.db.Flush(&grocksdb.FlushOptions{})
}

func (db QRocksDB) GetRecord(topic string, fragmentId uint32, offset uint64) (*grocksdb.Slice, error) {
	key := NewRecordKeyFromData(topic, fragmentId, offset)
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[RecordCF], key.Data())
}

func (db QRocksDB) PutRecord(topic string, fragmentId uint32, offset uint64, nodeId string, seqNum uint64, data []byte) error {
	if len(nodeId) != 32 {
		return errors.New("invalid length for node id")
	}
	key := NewRecordKeyFromData(topic, fragmentId, offset)
	value := NewRecordValueFromData(nodeId, seqNum, data)

	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data(), value.Data())
}

func (db QRocksDB) DeleteRecord(topic string, fragmentId uint32, offset uint64) error {
	key := NewRecordKeyFromData(topic, fragmentId, offset)
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data())
}

func (db *QRocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
}

func (db *QRocksDB) Destroy() error {
	return grocksdb.DestroyDb(db.dbPath, grocksdb.NewDefaultOptions())
}

func (db QRocksDB) ColumnFamilyHandles() grocksdb.ColumnFamilyHandles {
	return db.columnFamilyHandles
}

func (db QRocksDB) Scan(cfIndex CFIndex) *grocksdb.Iterator {
	return db.db.NewIteratorCF(db.ro, db.ColumnFamilyHandles()[cfIndex])
}

type RecordKey struct {
	*grocksdb.Slice
	data    []byte
	isSlice bool
}

func NewRecordKeyFromData(topic string, fragmentId uint32, offset uint64) *RecordKey {
	data := make([]byte, len(topic)+1+uint32Len+uint64Len)
	copy(data, topic+"@")
	binary.BigEndian.PutUint32(data[len(topic)+1:], fragmentId)
	binary.BigEndian.PutUint64(data[len(topic)+1+uint32Len:], offset)
	return &RecordKey{data: data, isSlice: false}
}

func NewRecordKey(slice *grocksdb.Slice) *RecordKey {
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
	return string(k.Data()[:k.Size()-uint32Len-uint64Len-1])
}

func (k RecordKey) FragmentId() uint32 {
	return binary.BigEndian.Uint32(k.Data()[k.Size()-uint64Len-uint32Len : k.Size()-uint64Len])
}

func (k RecordKey) Offset() uint64 {
	return binary.BigEndian.Uint64(k.Data()[k.Size()-uint64Len:])
}

type RecordValue struct {
	*grocksdb.Slice
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

func NewRecordValue(slice *grocksdb.Slice) *RecordValue {
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
