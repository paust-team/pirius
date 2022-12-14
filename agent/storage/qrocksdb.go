package storage

import (
	"bytes"
	"errors"
	"github.com/linxGnu/grocksdb"
	"path/filepath"
	"time"
	"unsafe"
)

var uint32Len = int(unsafe.Sizeof(uint32(0)))
var uint64Len = int(unsafe.Sizeof(uint64(0)))

func GetNowTimestamp() uint64 {
	now := time.Now()
	return uint64(now.Unix())
}

type CFIndex int

const (
	DefaultCF CFIndex = iota
	RecordCF
	RecordExpCF // column family for record-expiration
)

var columnFamilies = []string{
	"default",
	"record",
	"record_exp",
}

func (c CFIndex) String() string { return columnFamilies[c] }

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

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	blockCache := grocksdb.NewLRUCache(1 << 18)
	bbto.SetBlockCache(blockCache)

	defaultOpts := grocksdb.NewDefaultOptions()
	defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)
	defaultOpts.SetCompression(grocksdb.SnappyCompression)
	defaultOpts.SetMaxOpenFiles(16)
	opts := grocksdb.NewDefaultOptions()
	db, columnFamilyHandles, err := grocksdb.OpenDbColumnFamilies(defaultOpts, dbPath, columnFamilies, []*grocksdb.Options{opts, opts, opts})
	if err != nil {
		return nil, err
	}

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetTailing(true)
	wo := grocksdb.NewDefaultWriteOptions()
	return &QRocksDB{dbPath: dbPath, db: db, ro: ro, wo: wo, columnFamilyHandles: columnFamilyHandles}, nil
}

func (db *QRocksDB) Flush() error {
	return db.db.Flush(&grocksdb.FlushOptions{})
}

func (db *QRocksDB) GetRecord(topic string, fragmentId uint32, offset uint64) (*grocksdb.Slice, error) {
	key := NewRecordKeyFromData(topic, fragmentId, offset)
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[RecordCF], key.Data())
}

// PutRecord expirationDate is timestamp(second) type
func (db *QRocksDB) PutRecord(topic string, fragmentId uint32, offset uint64, seqNum uint64, data []byte, expirationDate uint64) error {
	if expirationDate <= GetNowTimestamp() {
		return errors.New("invalid retentionPeriod: expiration date should be greater than current timestamp")
	}

	// put record
	key := NewRecordKeyFromData(topic, fragmentId, offset)
	value := NewRecordValueFromData(seqNum, data)
	if err := db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordCF], key.Data(), value.Data()); err != nil {
		return err
	}

	// put retention period
	retentionKey := NewRetentionPeriodKeyFromData(key, expirationDate)
	if err := db.db.PutCF(db.wo, db.ColumnFamilyHandles()[RecordExpCF], retentionKey.Data(), []byte{}); err != nil {
		return err
	}
	return nil
}

// DeleteExpiredRecords Record only can be deleted on expired
func (db *QRocksDB) DeleteExpiredRecords() (deletedCount int, deletionErr error) {
	it := db.Scan(RecordExpCF)
	defer it.Close()
	now := GetNowTimestamp()
	var (
		startRetentionKey, endRetentionKey []byte
	)

	for it.SeekToFirst(); it.Valid(); it.Next() {
		retentionKey := NewRetentionPeriodKey(it.Key())
		if retentionKey.ExpirationDate() <= now {
			if err := db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordCF], retentionKey.RecordKey().Data()); err != nil {
				deletionErr = err
				continue
			}
			if startRetentionKey == nil {
				startRetentionKey = append(make([]byte, 0, len(retentionKey.Data())), retentionKey.Data()...)
			}
			endRetentionKey = append(make([]byte, 0, len(retentionKey.Data())), retentionKey.Data()...)
			deletedCount++
		}
		retentionKey.Free()
	}

	if bytes.Compare(startRetentionKey, endRetentionKey) == 0 {
		deletionErr = db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordExpCF], startRetentionKey)
	} else {
		deletionErr = db.db.DeleteRangeCF(db.wo, db.ColumnFamilyHandles()[RecordExpCF], startRetentionKey, endRetentionKey)
		deletionErr = db.db.DeleteCF(db.wo, db.ColumnFamilyHandles()[RecordExpCF], endRetentionKey)
	}
	return
}

func (db *QRocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
}

func (db *QRocksDB) Destroy() error {
	return grocksdb.DestroyDb(db.dbPath, grocksdb.NewDefaultOptions())
}

func (db *QRocksDB) ColumnFamilyHandles() grocksdb.ColumnFamilyHandles {
	return db.columnFamilyHandles
}

func (db *QRocksDB) Scan(cfIndex CFIndex) *grocksdb.Iterator {
	return db.db.NewIteratorCF(db.ro, db.ColumnFamilyHandles()[cfIndex])
}
