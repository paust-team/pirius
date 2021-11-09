package common

import "testing"

func TestMakeTopicMeta(t *testing.T) {
	var expectedLastOffset uint64 = 100
	var expectedDescription = "test topic meta"
	var expectedNumPartitions uint32 = 0
	var expectedReplicationFactor uint32 = 0
	var expectedNumPublishers uint64 = 1
	var expectedNumSubscribers uint64 = 1

	meta := NewTopicMetaFromValues(expectedDescription, expectedNumPartitions, expectedReplicationFactor,
		expectedLastOffset, expectedNumPublishers, expectedNumSubscribers)

	if meta.Description() != expectedDescription {
		t.Error("Description is not equal")
	}

	if meta.NumPartitions() != expectedNumPartitions {
		t.Error("Num Partition is not equal")
	}

	if meta.ReplicationFactor() != expectedReplicationFactor {
		t.Error("Replication Factor is not equal")
	}

	if meta.LastOffset() != expectedLastOffset {
		t.Error("Last Offset is not equal")
	}
}
