package dbsvc

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Resolve the attribute(s) in the document structure along the given path.
func GetIn(doc interface{}, path []string) (ret []interface{}) {
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		tdlog.Printf("%v cannot be indexed because type conversation to map[string]interface{} failed", doc)
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

// Return string hash code using sdbm algorithm.
func StrHash(thing interface{}) int {
	var hash rune
	for _, c := range fmt.Sprint(thing) {
		hash = c + (hash << 6) + (hash << 16) - hash
	}
	return int(hash)
}

// Insert a document.
func (db *DBSvc) DocInsert(colName string, doc map[string]interface{}) error {
	return nil
}

func (db *DBSvc) DocRead(colName string, id int) (doc map[string]interface{}, err error) {
	return
}

func (db *DBSvc) DocUpdate(colName string, id int, newDoc map[string]interface{}) error {
	return nil
}

func (db *DBSvc) DocDelete(colName string, id int) error {
	return nil
}
