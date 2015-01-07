/* Features in addition to single-shard implementation, to support multi-shard environment. */
package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"path"
	"strings"
)

// Return reference to hash table by joint index path.
func (col *Col) BPUseHT(jointPath string) *data.HashTable {
	col.db.lock.RLock()
	ret := col.hts[jointPath]
	col.db.lock.RUnlock()
	return ret
}

// Record placement of a document lock.
func (col *Col) BPLock(id uint64) error {
	if _, locked := col.locked[id]; locked {
		return fmt.Errorf("Document %d is currently locked", id)
	}
	col.locked[id] = struct{}{}
	return nil
}

// Remove a document lock.
func (col *Col) BPUnlock(id uint64) {
	delete(col.locked, id)
}

// Insert a new document and place a lock on it.
func (col *Col) BPLockAndInsert(id uint64, doc []byte) (err error) {
	if err = col.BPLock(id); err != nil {
		return err
	}
	_, err = col.part.Insert(id, doc)
	return
}

// Retrieve a document by ID, return raw document content.
func (col *Col) BPRead(id uint64) (doc []byte, err error) {
	doc, err = col.part.Read(id)
	return
}

// Retrieve a document by ID and place a lock on it.
func (col *Col) BPLockAndRead(id uint64) (doc []byte, err error) {
	if err = col.BPLock(id); err != nil {
		return
	}
	doc, err = col.part.Read(id)
	return
}

// Update a document by ID.
func (col *Col) BPUpdate(id uint64, newDoc []byte) (err error) {
	err = col.part.Update(id, newDoc)
	return
}

func (col *Col) BPDelete(id uint64) {
	col.part.Delete(id)
}

func (col *Col) BPApproxDocCount() uint64 {
	return col.part.ApproxDocCount()
}

// Install an index without reindexing documents.
func (col *Col) BPIndex(idxPath []string) (err error) {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; exists {
		return fmt.Errorf("Path %v is already indexed", idxPath)
	}
	col.indexPaths[idxName] = idxPath
	idxFileName := path.Join(col.db.path, col.name, idxName)
	col.hts[idxName], err = data.OpenHashTable(idxFileName)
	return
}
