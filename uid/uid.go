package uid

import (
	"fmt"
	"math/rand"
	"strconv"
)

const (
	PK_NAME = "@id" // Name of UID (PK) attribute
)

// Generate and return a new UID (Unique IDentifier).
func NextUID() uint64 {
	return uint64(rand.Int63()) + uint64(rand.Int63())
}

// Return value of the PK attribute in the document.
func PKOfDoc(doc map[string]interface{}, panicOnErr bool) uint64 {
	docPK, ok := doc[PK_NAME].(string)
	if !ok {
		if panicOnErr {
			panic(fmt.Sprintf("Doc %v does not have a valid PK", doc))
		}
		return 18446744073709551615
	}
	strint, err := strconv.ParseUint(docPK, 10, 64)
	if err != nil {
		if panicOnErr {
			panic(fmt.Sprintf("Doc %v does not have a valid PK", doc))
		}
		return 18446744073709551615
	}
	return strint
}
