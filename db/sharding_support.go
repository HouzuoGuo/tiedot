/* Features supporting multi-shard environment in addition to the ordinary single-shard implementation. */
package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"path"
	"strings"
)

// Return reference to hash table.
func (col *Col) MultiShardUseHT(jointPath string) *data.HashTable {
	ret := col.hts[jointPath]
	return ret
}

// Place a document lock by memorizing its ID in an internal structure.
func (col *Col) MultiShardLockDoc(id uint64) error {
	if _, locked := col.locked[id]; locked {
		return fmt.Errorf("Document %d is currently locked", id)
	}
	col.locked[id] = struct{}{}
	return nil
}

// Remove a document ID from the internal lock structure.
func (col *Col) MultiShardUnlockDoc(id uint64) {
	delete(col.locked, id)
}

// Insert a new document and immediately place a lock on it.
func (col *Col) MultiShardLockDocAndInsert(id uint64, doc []byte) (err error) {
	if err = col.MultiShardLockDoc(id); err != nil {
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

// Retrieve a document by ID and immediately place a lock on it.
func (col *Col) MultiShardLockDocAndRead(id uint64) (doc []byte, err error) {
	if err = col.MultiShardLockDoc(id); err != nil {
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

// Delete a document by ID.
func (col *Col) BPDelete(id uint64) {
	col.part.Delete(id)
}

// Return approximate number of documents in the partition.
func (col *Col) BPApproxDocCount() uint64 {
	return col.part.ApproxDocCount()
}

// Install an index without re-indexing any document.
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
