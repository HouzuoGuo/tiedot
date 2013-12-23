package tdlog

import (
	"log"
)

var VerboseLog bool = true

// Write a non-fatal advisory log message
func Printf(template string, params ...interface{}) {
	if VerboseLog {
		log.Printf(template, params...)
	}
}
func Println(line ...interface{}) {
	if VerboseLog {
		log.Println(line...)
	}
}

// Write an error log message, but continue program execution
func Error(line ...interface{}) {
	log.Println(line...)
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
