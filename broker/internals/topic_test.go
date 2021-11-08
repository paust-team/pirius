package internals

import "testing"

func TestMakeTopicMeta(t *testing.T) {
	var expectedLastOffset uint64 = 100
	var expectedDescription = "test topic meta"
	var expectedNumPartitions uint32 = 0
	var expectedReplicationFactor uint32 = 0

	meta := NewTopicMetaFromValues(expectedDescription, expectedNumPartitions, expectedReplicationFactor, expectedLastOffset)

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

func TestIncreaseLastOffset(t *testing.T) {
	var expectedLastOffset uint64 = 100
	var expectedDescription = "test topic meta"
	var expectedNumPartitions uint32 = 0
	var expectedReplicationFactor uint32 = 0

	testTopicName := "test-topic"

	meta := NewTopicMetaFromValues(expectedDescription, expectedNumPartitions, expectedReplicationFactor, expectedLastOffset)
	topic := NewTopic(testTopicName, meta)

	if topic.LastOffset() != expectedLastOffset {
		t.Fatal("Last Offset is not equal")
	}

	expectedLastOffset += 1

	topic.IncreaseLastOffset()
	if topic.LastOffset() != expectedLastOffset {
		t.Fatal("Increased Last Offset is not equal")
	}
	if topic.meta.LastOffset() != expectedLastOffset {
		t.Fatal("Increased Last Offset of topic-meta is not equal")
	}
}
