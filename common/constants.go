package common

var DefaultBrokerPort uint16 = 1101
var DefaultChunkSize uint32 = 1024

type BackPressureMode int
const (
	AtMostOnce BackPressureMode = iota
	AtLeastOnce
	ExactlyONce
)