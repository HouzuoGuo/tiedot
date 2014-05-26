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

// Put a document on all user-created indexes.
func (col *Col) indexDoc(id int, doc map[string]interface{}) {
	for idxName, idxPath := range col.indexPaths {
		for _, idxVal := range GetIn(doc, idxPath) {
			hashKey := StrHash(fmt.Sprint(idxVal))
			partNum := hashKey % col.db.numParts
			ht := col.hts[partNum][idxName]
			// fmt.Printf("Value %v (%d) goes to partition %d ht %s docID %d\n", idxVal, hashKey, partNum, idxName, id)
			ht.Lock.Lock()
			ht.Put(hashKey, id)
			ht.Lock.Unlock()
		}
	}
}

// Remove a document from all user-created indexes.
func (col *Col) unindexDoc(id int, doc map[string]interface{}) {
	for idxName, idxPath := range col.indexPaths {
		for _, idxVal := range GetIn(doc, idxPath) {
			hashKey := StrHash(fmt.Sprint(idxVal))
			partNum := hashKey % col.db.numParts
			ht := col.hts[partNum][idxName]
			ht.Lock.Lock()
			ht.Remove(hashKey, id)
			ht.Lock.Unlock()
		}
	}
}

// Insert a document with the specified ID into the collection. Take care of index but does not take care of locking.
func (col *Col) InsertRecovery(id int, doc map[string]interface{}) (err error) {
	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}
	partNum := id % col.db.numParts
	part := col.parts[partNum]
	// Put document data into collection
	if _, err = part.Insert(id, []byte(docJS)); err != nil {
		return
	}
	// Index the document
	col.indexDoc(id, doc)
	return
}

// Insert a document into the collection.
func (col *Col) Insert(doc map[string]interface{}) (id int, err error) {
	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}
	id = rand.Int()
	partNum := id % col.db.numParts
	part := col.parts[partNum]
	// Put document data into collection
	part.Lock.Lock()
	if _, err = part.Insert(id, []byte(docJS)); err != nil {
		part.Lock.Unlock()
		return
	}
	// If another thread is updating the document in the meanwhile,
	// it will take care of index maintenance
	if err = part.LockUpdate(id); err != nil {
		part.Lock.Unlock()
		return id, nil
	}
	part.Lock.Unlock()
	// Index the document
	col.indexDoc(id, doc)
	part.Lock.Lock()
	part.UnlockUpdate(id)
	part.Lock.Unlock()
	return
}

// Read a document and return it.
func (col *Col) Read(id int) (doc map[string]interface{}, err error) {
	part := col.parts[id%col.db.numParts]
	part.Lock.RLock()
	docB, err := part.Read(id)
	part.Lock.RUnlock()
	if err != nil {
		return
	}
	err = json.Unmarshal(docB, &doc)
	return
}

// Update a document.
func (col *Col) Update(id int, doc map[string]interface{}) error {
	if doc == nil {
		return fmt.Errorf("Updating %d: input doc may not be nil", id)
	}
	docJS, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	part := col.parts[id%col.db.numParts]
	part.Lock.Lock()
	// Place lock, read back original document and update
	if err := part.LockUpdate(id); err != nil {
		part.Lock.Unlock()
		return fmt.Errorf("Document %d is locked for update, please try again later", id)
	}
	originalB, err := part.Read(id)
	if err != nil {
		part.UnlockUpdate(id)
		part.Lock.Unlock()
		return fmt.Errorf("Cannot update %d: cannot read back original document (not found?)", err)
	}
	var original map[string]interface{}
	if err = json.Unmarshal(originalB, &original); err != nil {
		tdlog.Errorf("Will not attempt to unindex document %d during update", id)
	}
	if err = part.Update(id, []byte(docJS)); err != nil {
		part.UnlockUpdate(id)
		part.Lock.Unlock()
		return err
	}
	// Done with the data partition, next is to maintain indexed valued
	part.Lock.Unlock()
	if original != nil {
		col.unindexDoc(id, original)
	}
	col.indexDoc(id, doc)
	// Done with the document
	part.Lock.Lock()
	part.UnlockUpdate(id)
	part.Lock.Unlock()
	return nil
}

// Delete a document.
func (col *Col) Delete(id int) error {
	part := col.parts[id%col.db.numParts]
	part.Lock.Lock()
	// Place lock, read back original document and delete document
	if err := part.LockUpdate(id); err != nil {
		part.Lock.Unlock()
		return fmt.Errorf("Document %d is locked for update, please try again later", id)
	}
	originalB, err := part.Read(id)
	if err != nil {
		part.UnlockUpdate(id)
		part.Lock.Unlock()
		return fmt.Errorf("Cannot delete %d: cannot read back original document (not found?)", err)
	}
	var original map[string]interface{}
	if err = json.Unmarshal(originalB, &original); err != nil {
		tdlog.Errorf("Will not attempt to unindex document %d during delete", id)
	}
	if err = part.Delete(id); err != nil {
		part.UnlockUpdate(id)
		part.Lock.Unlock()
		return err
	}
	// Done with the partition, next is to remove indexed values
	part.Lock.Unlock()
	if original != nil {
		col.unindexDoc(id, original)
	}
	if part == nil {
		panic("SHALL NOT HAPPEN")
	}
	part.Lock.Lock()
	part.UnlockUpdate(id)
	part.Lock.Unlock()
	return nil
}
