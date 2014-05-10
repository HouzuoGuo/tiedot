package dbsvc

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net/rpc"
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

// Put a document on all indexes.
func (db *DBSvc) indexDoc(colName string, part *rpc.Client, id int, doc map[string]interface{}) error {
	for idxName, idxPath := range db.schema[colName] {
		for _, idxVal := range GetIn(doc, idxPath) {
			if err := part.Call("DataSvc.HTPut", datasvc.HTPutInput{idxName, StrHash(fmt.Sprint(idxVal)), id, db.mySchemaVersion}, discard); err != nil {
				return err
			}
		}
	}
	return nil
}

// Remove a document from all indexes.
func (db *DBSvc) unindexDoc(colName string, part *rpc.Client, id int, doc map[string]interface{}) error {
	for idxName, idxPath := range db.schema[colName] {
		for _, idxVal := range GetIn(doc, idxPath) {
			if err := part.Call("DataSvc.HTRemove", datasvc.HTRemoveInput{idxName, StrHash(fmt.Sprint(idxVal)), id, db.mySchemaVersion}, discard); err != nil {
				return err
			}
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
	defer db.lock.Unlock()
	if _, exists := db.schema[colName]; !exists {
		return 0, fmt.Errorf("Collection %s does not exist", colName)
	}
	db.lockPart(part)
	defer db.unlockPart(part)
	if err = db.callPartHandleReload(part, "DataSvc.DocInsert", &datasvc.DocInsertInput{colName, string(docJS), id, db.mySchemaVersion}, discard); err != nil {
		return
	}
	err = db.indexDoc(colName, part, id, doc)
	return
}

// Read a document by ID.
func (db *DBSvc) DocRead(colName string, id int) (doc map[string]interface{}, err error) {
	partNum := id % db.totalRank
	part := db.data[partNum]
	db.lock.Lock()
	defer db.lock.Unlock()
	if _, exists := db.schema[colName]; !exists {
		return nil, fmt.Errorf("Collection %s does not exist", colName)
	}
	db.lockPart(part)
	defer db.unlockPart(part)
	var docStr string
	if err = db.callPartHandleReload(part, "DataSvc.DocRead", datasvc.DocReadInput{colName, id, db.mySchemaVersion}, &docStr); err != nil {
		return
	}
	if err = json.Unmarshal([]byte(docStr), &doc); err != nil {
		return
	}
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
	defer db.lock.Unlock()
	if _, exists := db.schema[colName]; !exists {
		return fmt.Errorf("Collection %s does not exist", colName)
	}
	db.lockPart(part)
	defer db.unlockPart(part)
	// Read original document and remove it from all indexes
	var docStr string
	var oldDoc map[string]interface{}
	if err := db.callPartHandleReload(part, "DataSvc.DocRead", datasvc.DocReadInput{colName, id, db.mySchemaVersion}, &docStr); err != nil {
		return err
	} else if err := json.Unmarshal([]byte(docStr), &oldDoc); err != nil {
		return err
	} else if err := db.unindexDoc(colName, part, id, oldDoc); err != nil {
		return err
		// Then update the document and put it on all indexes
	} else if err := part.Call("DataSvc.DocUpdate", datasvc.DocUpdateInput{colName, string(docJS), id, db.mySchemaVersion}, discard); err != nil {
		return err
	} else if err := db.indexDoc(colName, part, id, newDoc); err != nil {
		return err
	}
	return nil
}

// Delete a document by ID.
func (db *DBSvc) DocDelete(colName string, id int) error {
	partNum := id % db.totalRank
	part := db.data[partNum]
	db.lock.Lock()
	defer db.lock.Unlock()
	if _, exists := db.schema[colName]; !exists {
		return fmt.Errorf("Collection %s does not exist", colName)
	}
	db.lockPart(part)
	defer db.unlockPart(part)
	// Read original document and remove it from all indexes
	var docStr string
	var oldDoc map[string]interface{}
	if err := db.callPartHandleReload(part, "DataSvc.DocRead", datasvc.DocReadInput{colName, id, db.mySchemaVersion}, &docStr); err != nil {
		return err
	} else if err := json.Unmarshal([]byte(docStr), &oldDoc); err != nil {
		println(docStr)
		return err
	} else if err := db.unindexDoc(colName, part, id, oldDoc); err != nil {
		return err
		// Then delete the document
	} else if err := part.Call("DataSvc.DocDelete", datasvc.DocDeleteInput{colName, id, db.mySchemaVersion}, discard); err != nil {
		return err
	}
	return nil
}
