package logger

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
)

type LogLevel int

const (
	Debug LogLevel = iota
	Info
	Warning
	Error
)

var logLevelMap = map[string]LogLevel{
	"DEBUG":   Debug,
	"INFO":    Info,
	"WARNING": Warning,
	"ERROR":   Error,
}

func LogLevelToString(logLevel LogLevel) string {

	for name, level := range logLevelMap {
		if level == logLevel {
			return name
		}
	}
	return "UNKNOWN"
}

func LogLevelFromString(logLevelString string) LogLevel {

	level, exists := logLevelMap[logLevelString]
	if !exists {
		return -1
	}
	return level
}

const (
	defaultLogFormat  = "#%.3[1]d %[2]s %[3]s [%[4]s]: %[5]s"
	defaultTimeFormat = "2006-01-02 15:04:05"
)

type QLogger struct {
	*log.Logger
	id          int
	logLevel    LogLevel
	packageName string
	fileName    string
	file        *os.File
}

func NewQLogger(packageName string, logLevel LogLevel) *QLogger {
	rand.Seed(time.Now().UnixNano())

	return &QLogger{
		id:          rand.Intn(900) + 100,
		packageName: packageName,
		logLevel:    logLevel,
		Logger:      log.New(os.Stderr, "", 0),
		file:        nil,
	}
}

func (l *QLogger) Inherit(parent *QLogger) {
	l.Logger = parent.Logger
	l.SetLogLevel(parent.logLevel)
}

func (l *QLogger) WithFile(logPath string) *QLogger {
	fpLog, err := os.OpenFile(logPath+"/log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	l.file = fpLog

	multiWriter := io.MultiWriter(fpLog, os.Stderr)
	l.Logger.SetOutput(multiWriter)

	return l
}

func (l *QLogger) Close() {
	l.Debug("close logger")
	if l.file != nil {
		_ = l.file.Close()
	}
}

func (l *QLogger) PrintTrace(print bool) {
	if print {
		l.Logger.SetFlags(log.Llongfile)
	} else {
		l.Logger.SetFlags(0)
	}
}

func (l *QLogger) SetLogLevel(logLevel LogLevel) {
	l.logLevel = logLevel
}

func (l *QLogger) log(level LogLevel, msg string) {
	if level < l.logLevel {
		return
	}
	tFmt := time.Now().Format(defaultTimeFormat)
	_ = l.Output(3, fmt.Sprintf(defaultLogFormat, l.id, tFmt, l.packageName, LogLevelToString(level), msg))
}

func (l *QLogger) Debug(v ...interface{}) {
	l.log(Debug, fmt.Sprintln(v...))
}

func (l *QLogger) Debugf(format string, v ...interface{}) {
	l.log(Debug, fmt.Sprintf(format, v...))
}

func (l *QLogger) Info(v ...interface{}) {
	l.log(Info, fmt.Sprintln(v...))
}

func (l *QLogger) Infof(format string, v ...interface{}) {
	l.log(Info, fmt.Sprintf(format, v...))
}

func (l *QLogger) Warning(v ...interface{}) {
	l.log(Warning, fmt.Sprintln(v...))
}

func (l *QLogger) Warningf(format string, v ...interface{}) {
	l.log(Warning, fmt.Sprintf(format, v...))
}

func (l *QLogger) Error(v ...interface{}) {
	l.log(Error, fmt.Sprintln(v...))
}

func (l *QLogger) Errorf(format string, v ...interface{}) {
	l.log(Error, fmt.Sprintf(format, v...))
	l.Print()
}

func (l *QLogger) Print(v ...interface{}) {
	l.Info(v...)
}

func (l *QLogger) Printf(format string, v ...interface{}) {
	l.Infof(format, v...)
}

func (l *QLogger) Println(v ...interface{}) {
	l.Info(v...)
}

func (l *QLogger) Fatal(v ...interface{}) {
	l.Error(v...)
	os.Exit(1)
}

func (l *QLogger) Fatalf(format string, v ...interface{}) {
	l.Errorf(format, v...)
	os.Exit(1)
}

func (l *QLogger) Fatalln(v ...interface{}) {
	l.Error(v...)
	os.Exit(1)
}
