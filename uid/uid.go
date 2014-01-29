package uid

import (
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
func PKOfDoc(doc map[string]interface{}) (uid uint64, found bool) {
	docPK, ok := doc[PK_NAME].(string)
	if !ok {
		return 0, false
	}
	uid, err := strconv.ParseUint(docPK, 10, 64)
	if err != nil {
		return 0, false
	}
	return uid, true
}
