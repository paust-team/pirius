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
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

const (
	defaultLogFormat  = "#%.3[1]d %[2]s %[3]s [%[4]s]: %[5]s"
	defaultTimeFormat = "2006-01-02 15:04:05"
)

func logLevelString(logLevel LogLevel) string {

	logLevelStrings := []string{
		"Debug",
		"Info",
		"Warning",
		"Error",
	}
	if logLevel > -1 && int(logLevel) < len(logLevelStrings) {
		return logLevelStrings[logLevel]
	} else {
		return "Unknown"
	}
}

type QLogger struct {
	*log.Logger
	id 			int
	logLevel    LogLevel
	packageName string
	fileName    string
	timeFormat  string
	logFormat   string
	file 		*os.File
}

func NewQLogger(packageName string, logLevel LogLevel) *QLogger {
	rand.Seed(time.Now().UnixNano())

	return &QLogger{
		id: rand.Intn(900) + 100,
		packageName: packageName,
		logLevel: logLevel,
		Logger: log.New(os.Stderr, "", 0),
		timeFormat: defaultTimeFormat,
		logFormat: defaultLogFormat,
		file: nil,
	}
}

func (l *QLogger) Inherit(parent *QLogger) {
	l.Logger = parent.Logger
	l.SetLogLevel(parent.logLevel)
	l.SetTimeFormat(parent.timeFormat)
	l.SetLogFormat(parent.logFormat)
}

func (l *QLogger) WithFile(logPath string) *QLogger {
	fpLog, err := os.OpenFile(logPath+"/log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil { panic(err) }

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

func (l *QLogger) SetTimeFormat(timeFormat string) {
	l.timeFormat = timeFormat
}

func (l *QLogger) SetLogFormat(logFormat string) {
	l.logFormat = logFormat
}

func (l *QLogger) log(level LogLevel, msg string) {
	if level < l.logLevel {
		return
	}
	tFmt := time.Now().Format(l.timeFormat)
	_ = l.Output(3, fmt.Sprintf(l.logFormat, l.id, tFmt, l.packageName, logLevelString(level), msg))
}

func (l *QLogger) Debug(v ...interface{}) {
	l.log(LogLevelDebug, fmt.Sprintln(v...))
}

func (l *QLogger) DebugF(format string, v ...interface{}) {
	l.log(LogLevelDebug, fmt.Sprintf(format, v))
}

func (l *QLogger) Info(v ...interface{}) {
	l.log(LogLevelInfo, fmt.Sprintln(v...))
}

func (l *QLogger) InfoF(format string, v ...interface{}) {
	l.log(LogLevelInfo, fmt.Sprintf(format, v))
}

func (l *QLogger) Warning(v ...interface{}) {
	l.log(LogLevelWarning, fmt.Sprintln(v...))
}

func (l *QLogger) WarningF(format string, v ...interface{}) {
	l.log(LogLevelWarning, fmt.Sprintf(format, v))
}

func (l *QLogger) Error(v ...interface{}) {
	l.log(LogLevelError, fmt.Sprintln(v...))
}

func (l *QLogger) ErrorF(format string, v ...interface{}) {
	l.log(LogLevelError, fmt.Sprintf(format, v))
	l.Print()
}

func (l *QLogger) Print(v ...interface{}) {
	l.Info(v...)
}

func (l *QLogger) Printf(format string, v ...interface{}) {
	l.InfoF(format, v...)
}

func (l *QLogger) Println(v ...interface{}) {
	l.Info(v...)
}

func (l *QLogger) Fatal(v ...interface{}) {
	l.Error(v...)
	os.Exit(1)
}

func (l *QLogger) Fatalf(format string, v ...interface{}) {
	l.ErrorF(format, v...)
	os.Exit(1)
}

func (l *QLogger) Fatalln(v ...interface{}) {
	l.Error(v...)
	os.Exit(1)
}
