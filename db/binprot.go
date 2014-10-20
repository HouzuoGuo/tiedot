/* Document and collection management features for binprot server. */
package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"path"
	"strings"
)

func (col *Col) BPLock(id uint64) error {
	if _, locked := col.locked[id]; locked {
		return fmt.Errorf("Document %d is locked for update at the moment", id)
	}
	col.locked[id] = true
	return nil
}

func (col *Col) BPUnlock(id uint64) {
	delete(col.locked, id)
}

func (col *Col) BPLockAndInsert(id uint64, doc []byte) (err error) {
	if err = col.BPLock(id); err != nil {
		return err
	}
	_, err = col.part.Insert(id, doc)
	return
}

func (col *Col) BPRead(id uint64) (doc []byte, err error) {
	doc, err = col.part.Read(id)
	return
}

func (col *Col) BPLockAndRead(id uint64) (doc []byte, err error) {
	if err = col.BPLock(id); err != nil {
		return
	}
	doc, err = col.part.Read(id)
	return
}

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
