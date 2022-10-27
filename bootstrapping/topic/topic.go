package topic

import "encoding/json"

type Option byte

const (
	UniquePerFragment Option = 1 << iota // if this option set, topic record should not be duplicated in multiple fragments
)

type Frame struct {
	data []byte
}

func NewTopicFrame(description string, option Option) Frame {
	data := []byte{byte(option)}
	data = append(data, description...)
	return Frame{data: data}
}

func (t Frame) Data() []byte {
	return t.data
}

func (t Frame) Size() int {
	return len(t.data)
}

func (t Frame) Options() Option {
	return Option(t.Data()[0])
}

func (t Frame) Description() string {
	return string(t.Data()[1:])
}

type FragInfo struct {
	Active      bool   `json:"active"`
	PublisherId string `json:"pub-id"`
	Address     string `json:"addr"`
}

type FragMappingInfo map[uint]FragInfo // key is fragment-id

type FragmentsFrame struct {
	data []byte
}

func NewTopicFragmentsFrame(mappingInfo FragMappingInfo) FragmentsFrame {
	data, _ := json.Marshal(mappingInfo)
	return FragmentsFrame{data: data}
}

func (t FragmentsFrame) Data() []byte {
	return t.data
}

func (t FragmentsFrame) Size() int {
	return len(t.data)
}

func (t FragmentsFrame) FragMappingInfo() FragMappingInfo {
	m := make(FragMappingInfo)
	if err := json.Unmarshal(t.Data(), &m); err != nil {
		return nil
	}
	return m
}

type SubscriptionInfo map[string][]uint // key is subscriber id

type SubscriptionsFrame struct {
	data []byte
}

func NewTopicSubscriptionsFrame(info SubscriptionInfo) SubscriptionsFrame {
	data, _ := json.Marshal(info)
	return SubscriptionsFrame{data: data}
}

func (t SubscriptionsFrame) Data() []byte {
	return t.data
}

func (t SubscriptionsFrame) Size() int {
	return len(t.data)
}

func (t SubscriptionsFrame) SubscriptionInfo() SubscriptionInfo {
	m := make(SubscriptionInfo)
	if err := json.Unmarshal(t.Data(), &m); err != nil {
		return nil
	}
	return m
}
