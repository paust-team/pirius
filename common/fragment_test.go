package common

import "testing"

func TestMakeTopicFragmentData(t *testing.T) {
	var expectedLastOffset uint64 = 1
	var expectedNumSubscribers uint64 = 1

	data := NewFragmentDataFromValues(expectedLastOffset, expectedNumSubscribers)

	if data.LastOffset() != expectedLastOffset {
		t.Error("LastOffset is not equal")
	}

	if data.NumSubscribers() != expectedNumSubscribers {
		t.Error("NumSubscribers is not equal")
	}
}
