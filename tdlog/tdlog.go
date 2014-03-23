package tdlog

import (
	"log"
)

var VerboseLog bool = true
var TraceLog bool = true

// Write a non-fatal advisory log message
func Printf(template string, params ...interface{}) {
	if VerboseLog {
		log.Printf(template, params...)
	}
}
func Println(params ...interface{}) {
	if VerboseLog {
		log.Println(params...)
	}
}

// Write an error log message, but continue program execution
func Error(params ...interface{}) {
	log.Println(params...)
}
func Errorf(template string, params ...interface{}) {
	log.Printf(template, params...)
}

// Write a log message then abort.
func Fatal(reason interface{}) {
	log.Fatal(reason)
}

// Write a log message then panic.
func Panicf(template string, params ...interface{}) {
	log.Panicf(template, params...)
}

// Write a trace message, for debugging purpose.
func Trace(params ...interface{}) {
	if TraceLog {
		log.Println(params)
	}
}

// Write a trace message, for debugging purpose.
func Tracef(template string, params ...interface{}) {
	if TraceLog {
		log.Printf(template, params...)
	}
}
