package dberr

import "fmt"

type Error struct {
	Code        int
	Err         string
	WithDetails string
}

func (e Error) Fault(details ...interface{}) Error {
	e.WithDetails = fmt.Sprintf(e.Err, details...)
	return e
}

func (e Error) Error() string {
	if e.WithDetails != "" {
		return e.WithDetails
	}
	return e.Err
}

const (
	// IO error
	GeneralError = iota

	// Document errors
	DocDoesNotExist = iota
	DocTooLarge     = iota
	DocIsLocked     = iota

	// Query input errors
	QueryNeedIndex       = iota
	QueryMissingSubQuery = iota
	QueryMalformedInt    = iota
	QueryMissingParam    = iota
)

var (
	// IO error
	ErrorIO = Error{GeneralError, "IO error has occured, see log for more details.", ""}

	// Document errors
	ErrorNoDoc       = Error{DocDoesNotExist, "Document `%d` does not exist", ""}
	ErrorDocTooLarge = Error{DocTooLarge, "Document is too large. Max: `%d`, Given: `%d`", ""}
	ErrorDocLocked   = Error{DocIsLocked, "Document `%d` is locked for update - try again later", ""}

	// Query input errors
	ErrorNeedIndex         = Error{QueryNeedIndex, "Please index %v and retry query %v.", ""}
	ErrorExpectingSubQuery = Error{QueryMissingSubQuery, "Expecting a vector of sub-queries, but %v given.", ""}
	ErrorExpectingInt      = Error{QueryMalformedInt, "Expecting `%s` as an integer, but %v given.", ""}
	ErrorMissing           = Error{QueryMissingParam, "Missing `%s`", ""}
)
