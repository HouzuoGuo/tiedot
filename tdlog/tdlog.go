package tdlog

import (
	"log"
)

var VerboseLog bool = false

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

// Write an error log message, but continue program execution.
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
