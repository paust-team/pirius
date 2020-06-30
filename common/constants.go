package common

var DefaultBrokerPort uint = 1101
var DefaultTimeout uint = 3

type BackPressure int

const (
	AtMostOnce BackPressure = iota
	AtLeastOnce
	ExactlyONce
)
