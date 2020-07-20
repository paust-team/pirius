package common

import "os"

var (
	DefaultHomeDir           = os.ExpandEnv("$HOME/.shapleq")
	DefaultBrokerPort uint   = 1101
	DefaultTimeout    uint   = 3
	DefaultConfigName string = "config"
)

type BackPressure int

const (
	AtMostOnce BackPressure = iota
	AtLeastOnce
	ExactlyONce
)
