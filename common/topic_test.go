package common

import "testing"

func TestMakeTopicData(t *testing.T) {
	var expectedDescription = "test topic meta"
	var expectedNumFragments uint32 = 1
	var expectedReplicationFactor uint32 = 0
	var expectedNumPublishers uint64 = 1

	data := NewTopicDataFromValues(expectedDescription, expectedNumFragments, expectedReplicationFactor, expectedNumPublishers)

	if data.Description() != expectedDescription {
		t.Error("Description is not equal")
	}

	if data.NumFragments() != expectedNumFragments {
		t.Error("Num Fragment is not equal")
	}

	if data.ReplicationFactor() != expectedReplicationFactor {
		t.Error("Replication Factor is not equal")
	}

	if data.NumPublishers() != expectedNumPublishers {
		t.Error("NumPublisher is not equal")
	}
}
