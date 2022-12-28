package storage

import (
	"errors"
	"fmt"
	"github.com/linxGnu/grocksdb"
	"path/filepath"
	"runtime"
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
	dwo                 *grocksdb.WriteOptions
	fo                  *grocksdb.FlushOptions
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
	dwo := grocksdb.NewDefaultWriteOptions()
	dwo.SetLowPri(true)
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(false)

	return &QRocksDB{dbPath: dbPath, db: db, ro: ro, wo: wo, dwo: dwo, fo: fo, columnFamilyHandles: columnFamilyHandles}, nil
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
func (db *QRocksDB) DeleteExpiredRecords() (numDeleted int, deletionErr error) {
	it := db.Scan(RecordExpCF)
	defer it.Close()
	now := GetNowTimestamp()

	var accError []error
	for it.SeekToFirst(); it.Valid(); it.Next() {
		retentionKey := NewRetentionPeriodKey(it.Key())
		if retentionKey.ExpirationDate() <= now {
			if err := db.db.DeleteCF(db.dwo, db.ColumnFamilyHandles()[RecordCF], retentionKey.RecordKey().Data()); err != nil {
				accError = append(accError, err)
				continue
			}
			if err := db.db.DeleteCF(db.dwo, db.ColumnFamilyHandles()[RecordExpCF], retentionKey.Data()); err != nil {
				accError = append(accError, err)
				continue
			}
			numDeleted++
		}
		retentionKey.Free()
		runtime.Gosched()
	}
	if numDeleted > 0 {
		if err := db.db.FlushCF(db.ColumnFamilyHandles()[RecordCF], db.fo); err != nil {
			accError = append(accError, err)
		}
		if err := db.db.FlushCF(db.ColumnFamilyHandles()[RecordExpCF], db.fo); err != nil {
			accError = append(accError, err)
		}
	}
	if len(accError) > 0 {
		errorStr := ""
		for idx, err := range accError {
			errorStr += fmt.Sprintf("error no(%d): %s\n", idx, err.Error())
		}
		deletionErr = errors.New(errorStr)
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
