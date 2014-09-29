package data

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

var (
	ErrorNoDoc       = Error{"Document `%d` does not exist", ""}
	ErrorDocTooLarge = Error{"Document is too large. Max: `%d`, Given: `%d`", ""}
	ErrorDocLocked   = Error{"Documenta `%d` is already locked", ""}
	ErrorOpFailed    = Error{"OPeration did not complete successfully", ""}
)
