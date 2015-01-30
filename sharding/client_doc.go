// DB sharding via IPC using a binary protocol - index schema handling, document and index value manipulation.
package sharding

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
)

// Resolve the attribute(s) in the document structure along the given path.
func ResolveDocAttr(doc interface{}, path []string) (ret []interface{}) {
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		return
	}
	var thing interface{} = docMap
	// Get inside each path segment
	for i, seg := range path {
		if aMap, ok := thing.(map[string]interface{}); ok {
			thing = aMap[seg]
		} else if anArray, ok := thing.([]interface{}); ok {
			for _, element := range anArray {
				ret = append(ret, ResolveDocAttr(element, path[i:])...)
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
func StringHash(str string) uint64 {
	var hash uint64
	for _, c := range str {
		hash = uint64(c) + (hash << 6) + (hash << 16) - hash
	}
	return hash
}

// Given a document ID, calculate the server rank in which the document belongs, and also turn the ID into byte slice.
func (client *RouterClient) docID2RankBytes(id uint64) (rank int, idBytes []byte) {
	rank = int(id % uint64(client.nProcs))
	idBytes = Buint64(id)
	return
}

// Lookup collection ID by name. If the collection name is not found, ping the server once and retry.
func (client *RouterClient) colName2IDBytes(colName string) (colID int32, idBytes []byte, err error) {
	colID, exists := client.schema.colNameLookup[colName]
	if !exists {
		if err = client.ping(); err != nil {
			return
		} else if colID, exists = client.schema.colNameLookup[colName]; !exists {
			err = fmt.Errorf("Collection %s does not exist", colName)
			return
		}
	}
	idBytes = Bint32(colID)
	return
}

// Put a document on all indexes.
func (client *RouterClient) indexDoc(colID int32, docID uint64, doc interface{}) error {
	docIDBytes := Buint64(docID)
	for htID, path := range client.schema.indexPaths[colID] {
		htIDBytes := Bint32(htID)
		for _, val := range ResolveDocAttr(doc, path) {
			if val != nil {
				htKey := StringHash(fmt.Sprint(val))
				if _, _, err := client.sendCmd(int(htKey%uint64(client.nProcs)), false, C_HT_PUT, htIDBytes, Buint64(htKey), docIDBytes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Look for potential value matches among indexed values. Collision cases should be handled by caller.
func (client *RouterClient) hashLookup(htID int32, limit uint64, strKey string) (result []uint64, err error) {
	hashKey := StringHash(strKey)
	_, resp, err := client.sendCmd(int(hashKey%uint64(client.nProcs)), false, C_HT_GET, Bint32(htID), Buint64(hashKey), Buint64(limit))
	if err != nil {
		return
	}
	result = make([]uint64, len(resp))
	for i, docID := range resp {
		result[i] = Uint64(docID)
	}
	return
}

// Remove a document from all indexes.
func (client *RouterClient) unindexDoc(colID int32, docID uint64, doc interface{}) error {
	docIDBytes := Buint64(docID)
	for htID, path := range client.schema.indexPaths[colID] {
		htIDBytes := Bint32(htID)
		for _, val := range ResolveDocAttr(doc, path) {
			if val != nil {
				htKey := StringHash(fmt.Sprint(val))
				if _, _, err := client.sendCmd(int(htKey%uint64(client.nProcs)), false, C_HT_REMOVE, htIDBytes, Buint64(htKey), docIDBytes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Insert a document with the specified ID and put it onto all indexes. Used by Scrub for document recovery.
func (client *RouterClient) insertRecovery(colID int32, docID uint64, doc interface{}) error {
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	colIDBytes := Bint32(colID)
	docIDBytes := Buint64(docID)
	rank := int(docID % uint64(client.nProcs))
	if _, _, err := client.sendCmd(rank, false, C_DOC_INSERT, colIDBytes, docIDBytes, docBytes); err != nil {
		return err
	} else if err = client.indexDoc(colID, docID, doc); err != nil {
		return err
	} else if _, _, err = client.sendCmd(rank, false, C_DOC_UNLOCK, colIDBytes, docIDBytes); err != nil {
		return err
	}
	return nil
}

// Insert a document and put it onto all indexes.
func (client *RouterClient) Insert(colName string, doc map[string]interface{}) (docID uint64, err error) {
	docID = uint64(rand.Int63())
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return
	}
	rank, docIDBytes := client.docID2RankBytes(docID)
	client.opLock.Lock()
	colID, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	} else if _, _, err = client.sendCmd(rank, true, C_DOC_INSERT, colIDBytes, docIDBytes, docBytes); err != nil {
		client.opLock.Unlock()
		return
	} else if err = client.indexDoc(colID, docID, doc); err != nil {
		client.opLock.Unlock()
		return
	}
	_, _, err = client.sendCmd(rank, false, C_DOC_UNLOCK, colIDBytes, docIDBytes)
	client.opLock.Unlock()
	return
}

// Read a document by ID.
func (client *RouterClient) Read(colName string, docID uint64) (doc map[string]interface{}, err error) {
	rank, docIDBytes := client.docID2RankBytes(docID)
	client.opLock.Lock()
	_, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	_, resp, err := client.sendCmd(rank, true, C_DOC_READ, colIDBytes, docIDBytes)
	client.opLock.Unlock()
	if err != nil {
		return
	}
	err = json.Unmarshal(resp[0], &doc)
	return
}

// Update a document by ID.
func (client *RouterClient) Update(colName string, docID uint64, doc map[string]interface{}) (err error) {
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return
	}
	rank, docIDBytes := client.docID2RankBytes(docID)
	client.opLock.Lock()
	colID, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	// Lock and read original document
	_, resp, err := client.sendCmd(rank, true, C_DOC_LOCK_READ, colIDBytes, docIDBytes)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	var originalDoc map[string]interface{}
	if json.Unmarshal(resp[0], &originalDoc) != nil {
		tdlog.Noticef("Will not attempt to unindex document %d during update", docID)
	}
	// Update the document content
	if _, _, err = client.sendCmd(rank, false, C_DOC_UPDATE, colIDBytes, docIDBytes, docBytes); err != nil {
		client.opLock.Unlock()
		return
	}
	// Maintain indexed values and finally unlock the document
	if originalDoc != nil {
		if err = client.unindexDoc(colID, docID, originalDoc); err != nil {
			client.opLock.Unlock()
			return
		}
	}
	if err = client.indexDoc(colID, docID, doc); err != nil {
		client.opLock.Unlock()
		return
	}
	_, _, err = client.sendCmd(rank, false, C_DOC_UNLOCK, colIDBytes, docIDBytes)
	client.opLock.Unlock()
	return
}

// Delete a document by ID
func (client *RouterClient) Delete(colName string, docID uint64) (err error) {
	rank, docIDBytes := client.docID2RankBytes(docID)
	client.opLock.Lock()
	colID, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	// Lock and read original document
	_, resp, err := client.sendCmd(rank, true, C_DOC_LOCK_READ, colIDBytes, docIDBytes)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	var originalDoc map[string]interface{}
	// Will not attempt to unindex the original document if it became corrupted
	json.Unmarshal(resp[0], &originalDoc)
	// Remove the document
	if _, _, err = client.sendCmd(rank, false, C_DOC_DELETE, colIDBytes, docIDBytes); err != nil {
		client.opLock.Unlock()
		return
	}
	// Maintain indexed values and finally unlock the document
	if originalDoc != nil {
		if err = client.unindexDoc(colID, docID, originalDoc); err != nil {
			client.opLock.Unlock()
			return
		}
	}
	_, _, err = client.sendCmd(rank, false, C_DOC_UNLOCK, colIDBytes, docIDBytes)
	client.opLock.Unlock()
	return
}

// (Test case only) return an error if value is not uniquely indexed in the index.
func (client *RouterClient) valIsIndexed(colName string, idxPath []string, val interface{}, docID uint64) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	colID, _, err := client.colName2IDBytes(colName)
	if err != nil {
		return err
	}
	for htID, htPath := range client.schema.indexPaths[colID] {
		if len(htPath) != len(idxPath) {
			continue
		}
		pathMatch := true
		for i, seg := range idxPath {
			if htPath[i] != seg {
				pathMatch = false
			}
		}
		if !pathMatch {
			continue
		}
		vals, err := client.hashLookup(htID, 0, fmt.Sprint(val))
		if err != nil {
			return err
		} else if len(vals) != 1 || vals[0] != docID {
			return fmt.Errorf("Looking for %v docID %v in %v, but got result %v", val, docID, idxPath, vals)
		}
		return nil
	}
	return fmt.Errorf("Index not found")
}

// (Test case only) return an error if value appears in the index.
func (client *RouterClient) valIsNotIndexed(colName string, idxPath []string, val interface{}, docID uint64) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	colID, _, err := client.colName2IDBytes(colName)
	if err != nil {
		return err
	}
	for htID, htPath := range client.schema.indexPaths[colID] {
		if len(htPath) != len(idxPath) {
			continue
		}
		pathMatch := true
		for i, seg := range idxPath {
			if htPath[i] != seg {
				pathMatch = false
			}
		}
		if !pathMatch {
			continue
		}
		vals, err := client.hashLookup(htID, 0, fmt.Sprint(val))
		if err != nil {
			return err
		}
		for _, v := range vals {
			if v == docID {
				return fmt.Errorf("Looking for %v %v in %v (should not return any), but got result %v", val, docID, idxPath, vals)
			}
		}
		return nil
	}
	return fmt.Errorf("Index not found")
}

func (client *RouterClient) approxDocCount(colName string) (count uint64, err error) {
	_, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		return
	}
	_, resp, err := client.sendCmd(0, true, C_DOC_APPROX_COUNT, colIDBytes)
	count = Uint64(resp[0]) * uint64(client.nProcs)
	return
}

// Return approximate number of documents in the collection.
func (client *RouterClient) ApproxDocCount(colName string) (count uint64, err error) {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	return client.approxDocCount(colName)
}

// Get a page of documents, The return value contains document ID vs Bytes (deserialize == false) or ID vs JSON interface (deserialize == true).
func (client *RouterClient) getDocPage(colName string, page, total uint64, deserialize bool) (docs map[uint64]interface{}, err error) {
	docs = make(map[uint64]interface{})
	_, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		return
	}
	for i := 0; i < client.nProcs; i++ {
		var resp [][]byte
		_, resp, err = client.sendCmd(i, true, C_DOC_GET_PAGE, colIDBytes, Buint64(page), Buint64(total))
		if err != nil {
			return
		}
		for i := 0; i < len(resp); i += 2 {
			docID := Uint64(resp[i])
			docBytes := resp[i+1]
			if deserialize {
				var doc interface{}
				if err = json.Unmarshal(docBytes, &doc); err == nil {
					docs[docID] = doc
				} else {
					tdlog.CritNoRepeat("Client %d - found document corruption in collection %s ID %d while going through docs", client.id, colName, docID)
				}
			} else {
				docs[docID] = docBytes
			}
		}
	}
	return
}

// Divide collection into roughly equally sized pages and return a page of documents.
func (client *RouterClient) GetDocPage(colName string, page, total uint64) (docs map[uint64]interface{}, err error) {
	client.opLock.Lock()
	docs, err = client.getDocPage(colName, page, total, true)
	client.opLock.Unlock()
	return
}

// Run fun for all document ID vs document content (bytes).
func (client *RouterClient) forEachDocBytes(colName string, fun func(uint64, []byte) bool) (err error) {
	docCount, err := client.approxDocCount(colName)
	if err != nil {
		return err
	}
	// Go through 10k documents at a time
	total := docCount/10000 + 1
	for page := uint64(0); page < total; page++ {
		docs, err := client.getDocPage(colName, page, total, false)
		if err != nil {
			return err
		}
		for id, docBytes := range docs {
			if !fun(id, docBytes.([]byte)) {
				return nil
			}
		}
	}
	return nil
}

// Run fun for all document ID vs document content (JSON interface).
func (client *RouterClient) forEachDoc(colName string, fun func(uint64, interface{}) bool) (err error) {
	docCount, err := client.approxDocCount(colName)
	if err != nil {
		return err
	}
	// Go through 10k documents at a time
	total := docCount/10000 + 1
	for page := uint64(0); page < total; page++ {
		docs, err := client.getDocPage(colName, page, total, true)
		if err != nil {
			return err
		}
		for id, doc := range docs {
			if !fun(id, doc) {
				return nil
			}
		}
	}
	return nil
}
