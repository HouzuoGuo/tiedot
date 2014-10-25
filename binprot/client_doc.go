// Binary protocol over IPC - Document management features (client).
package binprot

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
)

// Given a document ID, calculate the server rank in which the document belongs, and also turn the ID into byte slice.
func (client *BinProtClient) docID2RankBytes(id uint64) (rank int, idBytes []byte) {
	rank = int(id % uint64(client.nProcs))
	idBytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(idBytes, id)
	return
}

// Lookup collection ID by name. If the collection name is not found, ping the server once and retry.
func (client *BinProtClient) colName2IDBytes(colName string) (colID int32, idBytes []byte, err error) {
	colID, exists := client.colNameLookup[colName]
	if !exists {
		if err = client.ping(); err != nil {
			return
		} else if colID, exists = client.colNameLookup[colName]; !exists {
			err = fmt.Errorf("Collection %s does not exist", colName)
			return
		}
	}
	idBytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(colID))
	return
}

// Put a document on all indexes.
func (client *BinProtClient) indexDoc(colID int32, docID uint64, doc map[string]interface{}) error {
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	for htID, path := range client.indexPaths[colID] {
		htIDBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(htIDBytes, uint32(htID))
		for _, val := range db.GetIn(doc, path) {
			if val != nil {
				htKey := db.StrHash(fmt.Sprint(val))
				htKeyBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(htKeyBytes, htKey)
				if _, _, err := client.sendCmd(int(htKey%uint64(client.nProcs)), false, C_HT_PUT, htIDBytes, htKeyBytes, docIDBytes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Remove a document from all indexes.
func (client *BinProtClient) unindexDoc(colID int32, docID uint64, doc map[string]interface{}) error {
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	for htID, path := range client.indexPaths[colID] {
		htIDBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(htIDBytes, uint32(htID))
		for _, val := range db.GetIn(doc, path) {
			if val != nil {
				htKey := db.StrHash(fmt.Sprint(val))
				htKeyBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(htKeyBytes, htKey)
				if _, _, err := client.sendCmd(int(htKey%uint64(client.nProcs)), false, C_HT_REMOVE, htIDBytes, htKeyBytes, docIDBytes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Insert a document and put it onto all indexes.
func (client *BinProtClient) Insert(colName string, doc map[string]interface{}) (docID uint64, err error) {
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
func (client *BinProtClient) Read(colName string, docID uint64) (doc map[string]interface{}, err error) {
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
func (client *BinProtClient) Update(colName string, docID uint64, doc map[string]interface{}) (err error) {
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
func (client *BinProtClient) Delete(colName string, docID uint64) (err error) {
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
		tdlog.Noticef("Will not attempt to unindex document %d during delete", docID)
	}
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

// Return an error if a value is not in index. Used only by test case.
func (client *BinProtClient) valIsIndexed(colName string, idxPath []string, val interface{}, docID uint64) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	colID, _, err := client.colName2IDBytes(colName)
	if err != nil {
		return err
	}
	for htID, htPath := range client.indexPaths[colID] {
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
		htIDBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(htIDBytes, uint32(htID))
		hashKey := db.StrHash(fmt.Sprint(val))
		hashKeyBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(hashKeyBytes, uint64(hashKey))
		limitBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(limitBytes, 0)
		_, resp, err := client.sendCmd(int(hashKey%uint64(client.nProcs)), false, C_HT_GET, htIDBytes, hashKeyBytes, limitBytes)
		if err != nil {
			return err
		}
		vals := make([]uint64, len(resp))
		for i, aVal := range resp {
			vals[i] = binary.LittleEndian.Uint64(aVal)
		}
		if len(vals) != 1 || vals[0] != docID {
			return fmt.Errorf("Looking for %v (%v) docID %v in %v, but got result %v", val, hashKey, docID, idxPath, vals)
		}
		return nil
	}
	return fmt.Errorf("Index not found")
}

// Return an error if a value is in index. Used only by test case.
func (client *BinProtClient) valIsNotIndexed(colName string, idxPath []string, val interface{}, docID uint64) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	colID, _, err := client.colName2IDBytes(colName)
	if err != nil {
		return err
	}
	for htID, htPath := range client.indexPaths[colID] {
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
		htIDBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(htIDBytes, uint32(htID))
		hashKey := db.StrHash(fmt.Sprint(val))
		hashKeyBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(hashKeyBytes, uint64(hashKey))
		limitBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(limitBytes, 0)
		_, resp, err := client.sendCmd(int(hashKey%uint64(client.nProcs)), false, C_HT_GET, htIDBytes, hashKeyBytes, limitBytes)
		if err != nil {
			return err
		}
		vals := make([]uint64, len(resp))
		for i, aVal := range resp {
			vals[i] = binary.LittleEndian.Uint64(aVal)
		}
		for _, v := range vals {
			if v == docID {
				return fmt.Errorf("Looking for %v %v %v in %v (should not return any), but got result %v", val, hashKey, docID, idxPath, vals)
			}
		}
		return nil
	}
	return fmt.Errorf("Index not found")
}

// Return an approximate number of documents in the collection.
func (client *BinProtClient) ApproxDocCount(colName string) (count uint64, err error) {
	client.opLock.Lock()
	_, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	_, resp, err := client.sendCmd(0, true, C_DOC_APPROX_COUNT, colIDBytes)
	count = binary.LittleEndian.Uint64(resp[0]) * uint64(client.nProcs)
	client.opLock.Unlock()
	return
}

func (client *BinProtClient) getDocPage(colName string, page, total uint64) (docs map[uint64]interface{}, err error) {
	docs = make(map[uint64]interface{})
	_, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		return
	}
	for i := 0; i < client.nProcs; i++ {
		_, resp, err := client.sendCmd(i, true, C_DOC_GET_PAGE, colIDBytes)
		if err != nil {
			return
		}
		for i := 0; i < len(resp); i += 2 {
			docID := binary.LittleEndian.Uint64(resp[i])
			docBytes := resp[i+1]
			var doc interface{}
			if err = json.Unmarshal(docBytes, &doc); err == nil {
				docs[docID] = doc
			} else {
				tdlog.CritNoRepeat("Client %d: found document corruption in collection %s ID %d while going through docs", client.id, colName, docID)
			}
		}
	}
}
