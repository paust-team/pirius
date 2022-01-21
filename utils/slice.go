package utils

// TODO:: use generics (go 1.18)
//type comparable interface {
//	type int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64
//}

func Contains(s []uint32, x uint32) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}
