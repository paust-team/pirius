package common

import "encoding/binary"

type FragmentData struct {
	data []byte
}

func NewFragmentData(data []byte) *FragmentData {
	return &FragmentData{data: data}
}

func NewFragmentDataFromValues(lastOffset uint64, numSubscribers uint64) *FragmentData {
	data := make([]byte, uint64Len*2)
	binary.BigEndian.PutUint64(data[0:], lastOffset)
	binary.BigEndian.PutUint64(data[uint64Len:uint64Len*2], numSubscribers)
	return &FragmentData{data: data}
}

func (f FragmentData) Data() []byte {
	return f.data
}

func (f FragmentData) Size() int {
	return len(f.data)
}

func (f FragmentData) LastOffset() uint64 {
	return binary.BigEndian.Uint64(f.Data()[:uint64Len])
}

func (f *FragmentData) SetLastOffset(offset uint64) {
	binary.BigEndian.PutUint64(f.data[0:uint64Len], offset)
}

func (f FragmentData) NumSubscribers() uint64 {
	return binary.BigEndian.Uint64(f.Data()[uint64Len : uint64Len*2])
}

func (f *FragmentData) SetNumSubscribers(num uint64) {
	binary.BigEndian.PutUint64(f.data[uint64Len:uint64Len*2], num)
}
