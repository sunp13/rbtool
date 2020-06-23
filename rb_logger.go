package rbtool

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger 生产者日志
type Logger struct {
	std          *log.Logger
	info         *log.Logger
	err          *log.Logger
	LogSucc      bool // 是否打印成功日志
	LogToConsole bool // 是否打印到控制台
}

// Info ...
func (l *Logger) Info(format string, v ...interface{}) {
	if l.LogToConsole {
		l.std.Output(3, fmt.Sprintf("[I] "+format, v...))
	}
	if l.info != nil {
		l.info.Output(3, fmt.Sprintf("[I] "+format, v...))
	}
}

// Error ...
func (l *Logger) Error(format string, v ...interface{}) {
	if l.LogToConsole {
		l.std.Output(3, fmt.Sprintf("[E] "+format, v...))
	}
	if l.err != nil {
		l.err.Output(3, fmt.Sprintf("[E] "+format, v...))
	}
}

// NewProducerLogger 生成新的logger
func NewProducerLogger(path, alias string) *Logger {
	flag := log.Ldate | log.Lmicroseconds | log.Lshortfile

	l := new(Logger)
	l.std = log.New(os.Stdout, "", flag)

	l.info = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.producer.info.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[I] ", flag)

	l.err = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.producer.error.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[E] ", flag)
	return l
}

// NewConsumerLogger 生成新的logger
func NewConsumerLogger(path, alias string) *Logger {
	flag := log.Ldate | log.Lmicroseconds | log.Lshortfile

	l := new(Logger)
	l.std = log.New(os.Stdout, "", flag)

	l.info = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.consumer.info.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[I] ", flag)

	l.err = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.consumer.error.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[E] ", flag)
	return l
}

// NewDefaultLogger ...
func NewDefaultLogger() *Logger {
	flag := log.Ldate | log.Lmicroseconds | log.Lshortfile
	l := new(Logger)
	l.std = log.New(os.Stdout, "", flag)
	return l
}
