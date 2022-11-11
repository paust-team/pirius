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

type FragState uint

const (
	Inactive FragState = iota
	Active
	Stale
)

type FragInfo struct {
	State       FragState `json:"state"`
	PublisherId string    `json:"pub-id"`
	Address     string    `json:"addr"`
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

type PublisherInfo struct {
	Address           string
	Alive             bool
	ActiveFragments   []uint
	InActiveFragments []uint
	StaleFragments    []uint
}

type PublisherInfoMap map[string]*PublisherInfo

func ConvertToPublisherInfo(fragMappings FragMappingInfo) (PublisherInfoMap, int) {
	publisherMap := make(PublisherInfoMap)
	numActivePublishers := 0
	for fragmentId, info := range fragMappings {
		if _, ok := publisherMap[info.PublisherId]; !ok {
			publisherMap[info.PublisherId] = &PublisherInfo{
				ActiveFragments:   []uint{},
				InActiveFragments: []uint{},
				StaleFragments:    []uint{},
			}
		}
		if info.State == Active {
			publisherMap[info.PublisherId].Address = info.Address
			publisherMap[info.PublisherId].Alive = true
			publisherMap[info.PublisherId].ActiveFragments = append(publisherMap[info.PublisherId].ActiveFragments, fragmentId)
			numActivePublishers++
		} else if info.State == Inactive {
			publisherMap[info.PublisherId].InActiveFragments = append(publisherMap[info.PublisherId].InActiveFragments, fragmentId)
		} else {
			publisherMap[info.PublisherId].StaleFragments = append(publisherMap[info.PublisherId].StaleFragments, fragmentId)
		}
	}
	return publisherMap, numActivePublishers
}
