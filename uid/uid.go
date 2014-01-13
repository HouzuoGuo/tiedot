package uid

import (
	"fmt"
	"math/rand"
	"strconv"
)

const (
	PK_NAME = "_pk" // Name of UID (PK) attribute
)

// Generate and return a new UID (Unique IDentifier).
func NextUID() int {
	return rand.Int()
}

// Return value of the PK attribute in the document.
func PKOfDoc(doc map[string]interface{}, panicOnErr bool) int {
	docPK, ok := doc[PK_NAME].(string)
	if !ok {
		if panicOnErr {
			panic(fmt.Sprintf("Doc %v does not have a valid PK", doc))
		}
		return -1
	}
	strint, err := strconv.Atoi(docPK)
	if err != nil {
		if panicOnErr {
			panic(fmt.Sprintf("Doc %v does not have a valid PK", doc))
		}
		return -1
	}
	return strint
}
