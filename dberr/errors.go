package dberr

import "fmt"

type errorType string

const (
	ErrorNil       errorType = ""
	ErrorUndefined errorType = "Unknown Error."

	// IO error
	// Document errors
	// Query input errors
	ErrorIO          errorType = "IO error has occured, see log for more details."
	ErrorNoDoc       errorType = "Document `%d` does not exist"
	ErrorDocTooLarge errorType = "Document is too large. Max: `%d`, Given: `%d`"
	ErrorDocLocked   errorType = "Document `%d` is locked for update - try again later"

	ErrorNeedIndex         errorType = "Please index %v and retry query %v."
	ErrorExpectingSubQuery errorType = "Expecting a vector of sub-queries, but %v given."
	ErrorExpectingInt      errorType = "Expecting `%s` as an integer, but %v given."
	ErrorMissing           errorType = "Missing `%s`"
)

func Make(err errorType, details ...interface{}) Error {
	return Error{err, details}
}

type Error struct {
	err     errorType
	details []interface{}
}

func (e Error) Error() string {
	return fmt.Sprintf(string(e.err), e.details...)
}

func Type(e error) errorType {
	if e == nil {
		return ErrorNil
	}

	if err, ok := e.(Error); ok {
		return err.err
	}
	return ErrorUndefined
}
