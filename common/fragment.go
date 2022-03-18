package common

import (
	"math/rand"
)

const MaxFragmentCount = 1 << 8

func GenerateFragmentId() uint32 {
	return uint32(rand.Intn(MaxFragmentCount)) + 1
}
