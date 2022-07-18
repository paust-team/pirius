package storage

import (
	"encoding/binary"
	"github.com/linxGnu/grocksdb"
)

type RetentionPeriodKey struct {
	*grocksdb.Slice
	data    []byte
	isSlice bool
}

func NewRetentionPeriodKeyFromData(recordKey *RecordKey, expirationDate uint64) *RetentionPeriodKey {
	data := make([]byte, uint64Len)
	binary.BigEndian.PutUint64(data, expirationDate)
	return &RetentionPeriodKey{data: append(data, recordKey.Data()...), isSlice: false}
}

func NewRetentionPeriodKey(slice *grocksdb.Slice) *RetentionPeriodKey {
	return &RetentionPeriodKey{Slice: slice, isSlice: true}
}

func (k RetentionPeriodKey) Data() []byte {
	if k.isSlice {
		return k.Slice.Data()
	}
	return k.data
}

func (k *RetentionPeriodKey) SetData(data []byte) {
	copy(k.data, data)
}

func (k RetentionPeriodKey) Size() int {
	if k.isSlice {
		return k.Slice.Size()
	}
	return len(k.data)
}

func (k RetentionPeriodKey) ExpirationDate() uint64 {
	return binary.BigEndian.Uint64(k.Data()[:uint64Len])
}

func (k RetentionPeriodKey) RecordKey() RecordKey {
	return RecordKey{
		data:    k.Data()[uint64Len:],
		isSlice: false,
	}
}
