package db

import "fmt"

type Error struct {
	Err         string
	WithDetails string
}

func (e *Error) Fault(details ...interface{}) *Error {
	e.WithDetails = fmt.Sprintf(e.Err, details...)
	return e
}

func (e Error) Error() string {
	if e.WithDetails != "" {
		return e.WithDetails
	}
	return e.Err
}
