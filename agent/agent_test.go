package agent

import (
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestShapleQAgent_PubSub(t *testing.T) {
	testTopicName := "test-topic"
	var testStartSeqNum uint64 = 1000
	var testBatchSize uint32 = 1
	var testFlushInterval uint32 = 0

	testRecords := [][]byte{
		{'g', 'o', 'o', 'g', 'l', 'e'},
		{'p', 'a', 'u', 's', 't', 'q'},
		{'1', '2', '3', '4', '5', '6'},
	}
	pubConfig := config.NewAgentConfig()
	pubConfig.SetPort(10010)
	pubConfig.SetDBName("test-pub-store")
	publisher := NewShapleQAgent(pubConfig)

	if err := publisher.Start(); err != nil {
		t.Error(err)
		return
	}

	subConfig := config.NewAgentConfig()
	subConfig.SetPort(10011)
	subConfig.SetDBName("test-sub-store")
	subscriber := NewShapleQAgent(subConfig)

	if err := subscriber.Start(); err != nil {
		t.Error(err)
		return
	}

	// publish
	sendCh := make(chan pubsub.TopicData)
	defer close(sendCh)
	if err := publisher.StartPublish(testTopicName, sendCh); err != nil {
		t.Fatal(err)
	}

	for i, record := range testRecords {
		sendCh <- pubsub.TopicData{
			SeqNum: uint64(i) + testStartSeqNum,
			Data:   record,
		}
	}

	// subscribe
	recvCh, err := subscriber.StartSubscribe(testTopicName, testBatchSize, testFlushInterval)
	if err != nil {
		t.Fatal(err)
	}

	idx := 0
	for subscriptionResult := range recvCh {
		assert.Equal(t, len(subscriptionResult), 1)
		assert.Equal(t, subscriptionResult[0].SeqNum, testStartSeqNum+uint64(idx))
		assert.Equal(t, subscriptionResult[0].Data, testRecords[idx])
		idx++
		if idx == len(testRecords) {
			break
		}
	}

	subscriber.Stop()
	publisher.Stop()

	time.Sleep(1 * time.Second)
	assert.False(t, publisher.running)
	assert.False(t, subscriber.running)
}
