/* Document management for binprot. */
package db

import "fmt"

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
	col.db.lock.Lock()
	if err = col.BPLock(id); err != nil {
		return err
	}
	_, err = col.part.Insert(id, doc)
	col.db.lock.Unlock()
	return
}

func (col *Col) BPRead(id uint64) (doc []byte, err error) {
	col.db.lock.RLock()
	doc, err = col.part.Read(id)
	col.db.lock.RUnlock()
	return
}

func (col *Col) BPLockAndRead(id uint64) (doc []byte, err error) {
	col.db.lock.Lock()
	if err = col.BPLock(id); err != nil {
		return
	}
	doc, err = col.part.Read(id)
	col.db.lock.Unlock()
	return
}

func (col *Col) BPUpdate(id uint64, newDoc []byte) (err error) {
	col.db.lock.Lock()
	err = col.part.Update(id, newDoc)
	col.db.lock.Unlock()
	return
}

func (col *Col) BPDelete(id uint64) {
	col.db.lock.Lock()
	col.part.Delete(id)
	col.db.lock.Unlock()
}

func (col *Col) BPIndexKV(idxName string, key, val uint64) {
	col.db.lock.Lock()
	col.hts[idxName].Put(key, val)
	col.db.lock.Unlock()
}

func (col *Col) BPUnindexKV(idxName string, key, val uint64) {
	col.db.lock.Lock()
	col.hts[idxName].Remove(key, val)
	col.db.lock.Unlock()
}
