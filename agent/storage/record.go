package storage

import (
	"encoding/binary"
	"github.com/linxGnu/grocksdb"
	"unsafe"
)

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

func (k *RecordKey) SetOffset(offset uint64) {
	binary.BigEndian.PutUint64(k.Data()[k.Size()-uint64Len:], offset)
}

type RecordValue struct {
	*grocksdb.Slice
	data    []byte
	isSlice bool
}

func NewRecordValueFromData(seqNum uint64, publishedData []byte) *RecordValue {
	meta := make([]byte, int(unsafe.Sizeof(seqNum)))
	binary.BigEndian.PutUint64(meta[0:], seqNum)

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

func (v RecordValue) SeqNum() uint64 {
	return binary.BigEndian.Uint64(v.Data()[0:int(unsafe.Sizeof(uint64(0)))])
}

func (v RecordValue) PublishedData() []byte {
	return v.Data()[int(unsafe.Sizeof(uint64(0))):]
}
