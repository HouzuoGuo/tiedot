// Binary protocol over IPC - Document management features (client).
package binprot

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
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
	rank, idBytes := client.docID2RankBytes(docID)
	client.opLock.Lock()
	colID, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	} else if _, _, err = client.sendCmd(rank, true, C_DOC_INSERT, colIDBytes, idBytes, docBytes); err != nil {
		client.opLock.Unlock()
		return
	} else if err = client.indexDoc(colID, docID, doc); err != nil {
		client.opLock.Unlock()
		return
	}
	_, _, err = client.sendCmd(rank, true, C_DOC_UNLOCK, colIDBytes, idBytes)
	client.opLock.Unlock()
	return
}

// Read a document by ID.
func (client *BinProtClient) Read(colName string, id uint64) (doc map[string]interface{}, err error) {
	rank, docIDBytes := client.docID2RankBytes(id)
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

func (client *BinProtClient) Update(colName string, id uint64, doc map[string]interface{}) error {
	return nil
}

func (client *BinProtClient) Delete(colName string, id uint64) error {
	return nil
}
