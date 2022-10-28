package storage

import (
	"encoding/gob"
	"github.com/paust-team/shapleq/helper"
	"os"
	"sync"
)

type TopicFragmentOffsets struct {
	*sync.Map
}

func NewTopicFragmentOffsets(m map[string]map[uint]uint64) TopicFragmentOffsets {
	sm := sync.Map{}
	for k, v := range m {
		sm.Store(k, v)
	}
	return TopicFragmentOffsets{Map: &sm}
}
func (o *TopicFragmentOffsets) ToMap() map[string]map[uint]uint64 {
	m := make(map[string]map[uint]uint64)
	o.Map.Range(func(k, v interface{}) bool {
		m[k.(string)] = v.(map[uint]uint64)
		return true
	})
	return m
}

type agentMeta struct {
	PubId      string
	SubId      string
	PubOffsets map[string]map[uint]uint64
	SubOffsets map[string]map[uint]uint64
}

func (a agentMeta) convert() AgentMeta {
	return AgentMeta{
		PublisherID:       a.PubId,
		SubscriberID:      a.SubId,
		PublishedOffsets:  NewTopicFragmentOffsets(a.PubOffsets),
		SubscribedOffsets: NewTopicFragmentOffsets(a.SubOffsets),
	}
}

type AgentMeta struct {
	PublisherID       string
	SubscriberID      string
	PublishedOffsets  TopicFragmentOffsets
	SubscribedOffsets TopicFragmentOffsets
}

func (a AgentMeta) convert() agentMeta {
	return agentMeta{
		PubId:      a.PublisherID,
		SubId:      a.SubscriberID,
		PubOffsets: a.PublishedOffsets.ToMap(),
		SubOffsets: a.SubscribedOffsets.ToMap(),
	}
}

func SaveAgentMeta(path string, meta AgentMeta) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	dataEncoder := gob.NewEncoder(f)
	return dataEncoder.Encode(meta.convert())
}

func LoadAgentMeta(path string) (AgentMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			meta := AgentMeta{
				PublisherID:       helper.GenerateNodeId(),
				SubscriberID:      helper.GenerateNodeId(),
				PublishedOffsets:  NewTopicFragmentOffsets(make(map[string]map[uint]uint64)),
				SubscribedOffsets: NewTopicFragmentOffsets(make(map[string]map[uint]uint64)),
			}
			if err = SaveAgentMeta(path, meta); err == nil {
				return meta, nil
			}
		}
		return AgentMeta{}, err
	}

	defer f.Close()
	dataDecoder := gob.NewDecoder(f)
	var aMeta agentMeta
	if err = dataDecoder.Decode(&aMeta); err != nil {
		return AgentMeta{}, err
	}
	return aMeta.convert(), nil
}
