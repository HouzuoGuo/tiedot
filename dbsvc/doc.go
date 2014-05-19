package dbsvc

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
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
	var hash int
	for _, c := range fmt.Sprint(thing) {
		hash = int(c) + (hash << 6) + (hash << 16) - hash
	}
	if hash < 0 {
		return -hash
	}
	return hash
}

// Put a document on all indexes.
func (db *DBSvc) indexDoc(colName string, id int, doc map[string]interface{}, placeLock bool) error {
	for idxName, idxPath := range db.schema[colName] {
		for _, idxVal := range GetIn(doc, idxPath) {
			hashKey := StrHash(fmt.Sprint(idxVal))
			hashPartNum := hashKey % db.totalRank
			hashPart := db.data[hashPartNum]
			if placeLock {
				db.lockPart(hashPart)
			}
			if err := hashPart.Call("DataSvc.HTPut", datasvc.HTPutInput{idxName, hashKey, id, db.mySchemaVersion}, discard); err != nil {
				if placeLock {
					db.unlockPart(hashPart)
				}
				return err
			}
			if placeLock {
				db.unlockPart(hashPart)
			}
		}
	}
	return nil
}

// Remove a document from all indexes.
func (db *DBSvc) unindexDoc(colName string, id int, doc map[string]interface{}) error {
	for idxName, idxPath := range db.schema[colName] {
		for _, idxVal := range GetIn(doc, idxPath) {
			hashKey := StrHash(fmt.Sprint(idxVal))
			hashPartNum := hashKey % db.totalRank
			hashPart := db.data[hashPartNum]
			db.lockPart(hashPart)
			if err := hashPart.Call("DataSvc.HTRemove", datasvc.HTRemoveInput{idxName, hashKey, id, db.mySchemaVersion}, discard); err != nil {
				db.unlockPart(hashPart)
				return err
			}
			db.unlockPart(hashPart)
		}
	}
	return nil
}

// Insert a document.
func (db *DBSvc) DocInsert(colName string, doc map[string]interface{}) (id int, err error) {
	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}
	id = rand.Int()
	partNum := id % db.totalRank
	part := db.data[partNum]
	db.lock.Lock()
	db.lockPart(part)
	if err = db.callPartHandleReload(part, "DataSvc.DocInsert", &datasvc.DocInsertInput{colName, string(docJS), id, db.mySchemaVersion}, discard); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return
	}
	db.unlockPart(part)
	err = db.indexDoc(colName, id, doc, true)
	db.lock.Unlock()
	return
}

// Read a document by ID.
func (db *DBSvc) DocRead(colName string, id int) (doc map[string]interface{}, err error) {
	partNum := id % db.totalRank
	part := db.data[partNum]
	db.lock.Lock()
	db.lockPart(part)
	var docStr string
	if err = db.callPartHandleReload(part, "DataSvc.DocRead", datasvc.DocReadInput{colName, id, db.mySchemaVersion}, &docStr); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return
	}
	if err = json.Unmarshal([]byte(docStr), &doc); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return
	}
	db.unlockPart(part)
	db.lock.Unlock()
	return
}

// Update a document by ID.
func (db *DBSvc) DocUpdate(colName string, id int, newDoc map[string]interface{}) error {
	docJS, err := json.Marshal(newDoc)
	if err != nil {
		return err
	}
	partNum := id % db.totalRank
	part := db.data[partNum]
	db.lock.Lock()
	var docStr string
	var oldDoc map[string]interface{}
	db.lockPart(part)
	// Read original document, then update document content
	if err := db.callPartHandleReload(part, "DataSvc.DocRead", datasvc.DocReadInput{colName, id, db.mySchemaVersion}, &docStr); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return err
	} else if err := part.Call("DataSvc.DocUpdate", datasvc.DocUpdateInput{colName, string(docJS), id, db.mySchemaVersion}, discard); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return err
	}
	db.unlockPart(part)
	// Remove old document from all indexes, and put new document onto indexes
	if err := json.Unmarshal([]byte(docStr), &oldDoc); err != nil {
		db.lock.Unlock()
		tdlog.Printf("DocUpdate %d: Overwrite corrupted document in %s", id, colName)
		return nil
	} else if err := db.unindexDoc(colName, id, oldDoc); err != nil {
		db.lock.Unlock()
		return err
		// Then update the document and put it on all indexes
	} else if err := db.indexDoc(colName, id, newDoc, true); err != nil {
		db.lock.Unlock()
		return err
	}
	db.lock.Unlock()
	return nil
}

// Delete a document by ID.
func (db *DBSvc) DocDelete(colName string, id int) error {
	partNum := id % db.totalRank
	part := db.data[partNum]
	db.lock.Lock()
	db.lockPart(part)
	// Read original document and delete it
	var docStr string
	var oldDoc map[string]interface{}
	if err := db.callPartHandleReload(part, "DataSvc.DocRead", datasvc.DocReadInput{colName, id, db.mySchemaVersion}, &docStr); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return err
	} else if err := part.Call("DataSvc.DocDelete", datasvc.DocDeleteInput{colName, id, db.mySchemaVersion}, discard); err != nil {
		db.unlockPart(part)
		db.lock.Unlock()
		return err
	}
	db.unlockPart(part)
	// Remove the original document from all indexes
	if err := json.Unmarshal([]byte(docStr), &oldDoc); err != nil {
		db.lock.Unlock()
		tdlog.Printf("DocDelete %d: Corrupted document is deleted from %s", id, colName)
		return err
	} else if err := db.unindexDoc(colName, id, oldDoc); err != nil {
		db.lock.Unlock()
		return err
	}
	db.lock.Unlock()
	return nil
}
