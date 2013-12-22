package tdlog

import (
	"log"
)

type TiedotLogger struct {
	Verbose bool
}

// Write a non-fatal advisory log message
func (*TiedotLogger) Printf(template string, params ...interface{}) {
	if DefaultLogger.Verbose {
		log.Printf(template, params...)
	}
}
func (*TiedotLogger) Println(line ...interface{}) {
	if DefaultLogger.Verbose {
		log.Println(line...)
	}
}

// Write an error log message, but continue program execution
func (*TiedotLogger) Error(line ...interface{}) {
	log.Println(line...)
}
func (*TiedotLogger) Errorf(template string, params ...interface{}) {
	log.Printf(template, params...)
}

// Write a log message then abort.
func (*TiedotLogger) Fatal(reason interface{}) {
	log.Fatal(reason)
}

// Write a log message then panic.
func (*TiedotLogger) Panicf(template string, params ...interface{}) {
	log.Panicf(template, params...)
}

// The default logger config
var DefaultLogger TiedotLogger = TiedotLogger{Verbose: true}

func Printf(template string, params ...interface{}) {
	DefaultLogger.Printf(template, params...)
}

func Println(line interface{}) {
	DefaultLogger.Println(line)
}

func Error(line ...interface{}) {
	DefaultLogger.Println(line...)
}
func Errorf(template string, params ...interface{}) {
	DefaultLogger.Printf(template, params...)
}

func Fatal(reason interface{}) {
	DefaultLogger.Fatal(reason)
}

func Panicf(template string, params ...interface{}) {
	DefaultLogger.Panicf(template, params...)
}
