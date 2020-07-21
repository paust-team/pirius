package common

import (
	"fmt"
	"os"
)

var (
	DefaultHomeDir                = os.ExpandEnv("$HOME/.shapleq")
	DefaultBrokerConfigDir        = fmt.Sprintf("%s/config/broker", DefaultHomeDir)
	DefaultBrokerPort      uint   = 1101
	DefaultTimeout         uint   = 3
	DefaultConfigName      string = "config"
)

type BackPressure int

const (
	AtMostOnce BackPressure = iota
	AtLeastOnce
	ExactlyONce
)
