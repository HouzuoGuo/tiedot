/* Single-shard - document management and index maintenance features */
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
)

// Resolve the attribute(s) in the document structure along the given path.
func GetIn(doc interface{}, path []string) (ret []interface{}) {
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		return
	}
	var thing interface{} = docMap
	// Get into each path segment
	for i, seg := range path {
		if aMap, ok := thing.(map[string]interface{}); ok {
			thing = aMap[seg]
		} else if anArray, ok := thing.([]interface{}); ok {
			for _, element := range anArray {
				ret = append(ret, GetIn(element, path[i:])...)
			}
			return ret
		} else {
			return nil
		}
	}
	switch thing.(type) {
	case []interface{}:
		return append(ret, thing.([]interface{})...)
	default:
		return append(ret, thing)
	}
}

// Hash a string using sdbm algorithm.
func StrHash(str string) uint64 {
	var hash uint64
	for _, c := range str {
		hash = uint64(c) + (hash << 6) + (hash << 16) - hash
	}
	return hash
}

// Insert a document, return its auto-assigned ID.
func (col *Col) Insert(doc map[string]interface{}) (id uint64, err error) {
	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}
	id = uint64(rand.Int63())
	// Put document data into collection
	if _, err = col.part.Insert(id, []byte(docJS)); err != nil {
		return
	}
	return
}

// Retrieve a document by ID.
func (col *Col) Read(id uint64) (doc map[string]interface{}, err error) {
	docB, err := col.part.Read(id)
	if err != nil {
		return
	}
	err = json.Unmarshal(docB, &doc)
	return
}

// Update a document by ID.
func (col *Col) Update(id uint64, doc map[string]interface{}) error {
	if doc == nil {
		return fmt.Errorf("Updating %d: input doc may not be nil", id)
	}
	docJS, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	originalB, err := col.part.Read(id)
	if err != nil {
		return err
	}
	var original map[string]interface{}
	if err = json.Unmarshal(originalB, &original); err != nil {
		tdlog.Noticef("Will not attempt to unindex document %d during update", id)
	}
	if err = col.part.Update(id, []byte(docJS)); err != nil {
		return err
	}
	return nil
}

// Delete a document.
func (col *Col) Delete(id uint64) error {
	return col.part.Delete(id)
}
