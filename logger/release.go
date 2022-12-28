//go:build release

package logger

import (
	"go.uber.org/zap"
)

var log *zap.Logger

func init() {
	//var err error
	//config := zap.NewProductionConfig()
	//encoderConfig := zap.NewProductionEncoderConfig()
	//encoderConfig.TimeKey = "timestamp"
	//encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	//encoderConfig.StacktraceKey = ""
	//config.Encoding = "console"
	//config.EncoderConfig = encoderConfig
	//
	//log, err = config.Build(zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel))
	//if err != nil {
	//	panic(err)
	//}
	var err error
	log, err = zap.NewDevelopment(zap.AddStacktrace(zap.ErrorLevel))
	if err != nil {
		panic(err)
	}
}

func Debug(message string, fields ...zap.Field) {
	log.Debug(message, fields...)
}

func Info(message string, fields ...zap.Field) {
	log.Info(message, fields...)
}

func Warn(message string, fields ...zap.Field) {
	log.Warn(message, fields...)
}

func Error(message string, fields ...zap.Field) {
	log.Error(message, fields...)
}
