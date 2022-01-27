package common

import (
	"encoding/binary"
	"math/rand"
)

const MaxFragmentCount = 1 << 8

func GenerateFragmentId() uint32 {
	return uint32(rand.Intn(MaxFragmentCount)) + 1
}

type FrameForFragment struct {
	data []byte
}

func NewFragmentData(data []byte) *FrameForFragment {
	return &FrameForFragment{data: data}
}

func NewFragmentDataFromValues(lastOffset uint64, numSubscribers uint64) *FrameForFragment {
	data := make([]byte, uint64Len*2)
	binary.BigEndian.PutUint64(data[0:], lastOffset)
	binary.BigEndian.PutUint64(data[uint64Len:uint64Len*2], numSubscribers)
	return &FrameForFragment{data: data}
}

func (f FrameForFragment) Data() []byte {
	return f.data
}

func (f FrameForFragment) Size() int {
	return len(f.data)
}

func (f FrameForFragment) LastOffset() uint64 {
	return binary.BigEndian.Uint64(f.Data()[:uint64Len])
}

func (f *FrameForFragment) SetLastOffset(offset uint64) {
	binary.BigEndian.PutUint64(f.data[0:uint64Len], offset)
}

func (f FrameForFragment) NumSubscribers() uint64 {
	return binary.BigEndian.Uint64(f.Data()[uint64Len : uint64Len*2])
}

func (f *FrameForFragment) SetNumSubscribers(num uint64) {
	binary.BigEndian.PutUint64(f.data[uint64Len:uint64Len*2], num)
}
