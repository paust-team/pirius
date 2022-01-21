package utils

import "testing"

func TestContains(t *testing.T) {
	testSlice := []uint32{1, 2, 3}
	var trueVal uint32 = 1
	var falseVal uint32 = 4
	if Contains(testSlice, trueVal) == false {
		t.Error("should be true")
	}
	if Contains(testSlice, falseVal) == true {
		t.Error("should be false")
	}
}
