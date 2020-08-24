package storage

import (
	"encoding/binary"
	"github.com/paust-team/shapleq/common"
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
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(20))
	bbto.SetNoBlockCache(true)
	bbto.SetBlockRestartInterval(4)
	bbto.SetIndexType(gorocksdb.KHashSearchIndexType)
	defaultOpts := gorocksdb.NewDefaultOptions()
	defaultOpts.SetAllowMmapReads(true)
	defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCompression(gorocksdb.NoCompression)
	defaultOpts.SetLevel0FileNumCompactionTrigger(1)
	defaultOpts.SetMaxBackgroundFlushes(16)
	defaultOpts.SetMaxBackgroundCompactions(16)
	defaultOpts.SetMaxOpenFiles(-1)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)
	defaultOpts.SetWriteBufferSize(1 << 30)
	defaultOpts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(common.MaxTopicLength + 1))
	defaultOpts.SetAllowMmapWrites(false)
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

func (db QRocksDB) PutRecord(topic string, offset uint64, data []byte) error {
	key := NewRecordKeyFromData(topic, offset)
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data(), data)
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

// TODO:: check topic length from caller side
// In this method, if the topic exceeds the maximum length(MaxTopicLength),
// an error should be returned or copied only up to the MaxTopicLength
func NewRecordKeyFromData(topic string, offset uint64) *RecordKey {

	if len(topic) > common.MaxTopicLength { // temporary
		log.Fatalf("topic length must be <= %d", common.MaxTopicLength)
	}

	topicBytesLength := common.MaxTopicLength + 1
	dataBytes := make([]byte, topicBytesLength+int(unsafe.Sizeof(offset)))
	copy(dataBytes, topic+"@")

	binary.BigEndian.PutUint64(dataBytes[topicBytesLength:], offset)
	return &RecordKey{data: dataBytes, isSlice: false}
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
